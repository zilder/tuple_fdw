#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"

#include "storage.h"


PG_MODULE_MAGIC;

#define ELOG_PREFIX "tuple_fdw: "


struct fdw_options
{
    char   *filename;
    List   *attrs_sorted;
    bool    use_mmap;
    int     lz4_acceleration;
};


void _PG_init(void);

/* FDW routines */
static void tupleGetForeignRelSize(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid foreigntableid);
static void tupleGetForeignPaths(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid);
static ForeignScan *tupleGetForeignPlan(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid,
                      ForeignPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      Plan *outer_plan);
static TupleTableSlot *tupleIterateForeignScan(ForeignScanState *node);
static void tupleBeginForeignScan(ForeignScanState *node, int eflags);
static void tupleEndForeignScan(ForeignScanState *node);
static List *tuplePlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index);
static void tupleBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo,
						List *fdw_private,
						int subplan_index,
						int eflags);
static TupleTableSlot *tupleExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot);
static void tupleEndForeignModify(EState *estate,
                      ResultRelInfo *resultRelInfo);


void
_PG_init(void)
{
    // Nothing yet
}

PG_FUNCTION_INFO_V1(tuple_fdw_handler);
Datum
tuple_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *routine = makeNode(FdwRoutine);

    routine->GetForeignRelSize = tupleGetForeignRelSize;
    routine->GetForeignPaths = tupleGetForeignPaths;
    routine->GetForeignPlan = tupleGetForeignPlan;
    routine->BeginForeignScan = tupleBeginForeignScan;
    routine->IterateForeignScan = tupleIterateForeignScan;
    routine->EndForeignScan = tupleEndForeignScan;
	routine->PlanForeignModify = tuplePlanForeignModify;
	routine->BeginForeignModify = tupleBeginForeignModify;
	routine->ExecForeignInsert = tupleExecForeignInsert;
	routine->EndForeignModify = tupleEndForeignModify;

    PG_RETURN_POINTER(routine);
}

static List *
parse_attributes_list(char *start, Oid relid)
{
    List       *attrs = NIL;
    char       *token;
    const char *delim = " ";
    AttrNumber  attnum;

    while ((token = strtok(start, delim)) != NULL)
    {
        if ((attnum = get_attnum(relid, token)) == InvalidAttrNumber)
            elog(ERROR, ELOG_PREFIX "invalid attribute name '%s'", token);
        attrs = lappend_int(attrs, attnum);
        start = NULL;
    }

    return attrs;
}

PG_FUNCTION_INFO_V1(tuple_fdw_validator);
Datum
tuple_fdw_validator(PG_FUNCTION_ARGS)
{
    List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *lc;
    bool        filename_provided = false;

    /* Only check table options */
    if (catalog != ForeignTableRelationId)
        PG_RETURN_VOID();

    foreach(lc, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            const char *filename = defGetString(def);

            if (access(filename, F_OK) == -1)
            {
                FILE   *fd;

                elog(WARNING,
                     ELOG_PREFIX "file '%s' does not exist; it will be created automatically",
                     filename);

                /* file does not exist, create one */
                if ((fd = AllocateFile(filename, "ab+")) == NULL)
                    elog(ERROR, "cannot open file '%s'", filename);

                FreeFile(fd);
            }
            filename_provided = true;
        }
        else if (strcmp(def->defname, "sorted") == 0)
        {
            /* 
             * TODO: we can't check that those are actual column names. But
             * at least we could verify that this is a correct space separated
             * list
             */
        }
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            defGetBoolean(def);
        }
        else if (strcmp(def->defname, "lz4_acceleration") == 0)
        {
            /* TODO: validate it's an integer */
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg(ELOG_PREFIX "unknown option: '%s'", def->defname)));
        }
    }

    if (!filename_provided)
        elog(ERROR, ELOG_PREFIX "filename is required");

    PG_RETURN_VOID();
}

static void
extract_table_options(Oid relid, struct fdw_options *options)
{
	ForeignTable   *table;
    ListCell       *lc;

    options->lz4_acceleration = 1;  /* default acceleration */

    table = GetForeignTable(relid);
    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            options->filename = defGetString(def);
        }
        else if (strcmp(def->defname, "sorted") == 0)
        {
            options->attrs_sorted =
                parse_attributes_list(defGetString(def), relid);
        }
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            options->use_mmap = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "lz4_acceleration") == 0)
        {
            options->lz4_acceleration = pg_atoi(defGetString(def), 4, 0);
        }
    }
}

static List *
fdw_options_to_list(struct fdw_options *o)
{
    List *lst = NIL;

    lst = lappend(lst, makeString(o->filename));
    lst = lappend(lst, makeInteger(o->use_mmap));
    lst = lappend(lst, makeInteger(o->lz4_acceleration));
    /* 
     * We don't pass `attrs_sorted` further to the executer as it is only used
     * in the planner
     */

    return lst;
}

static void
tupleGetForeignRelSize(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid foreigntableid)
{
    struct fdw_options *options;

    options = palloc0(sizeof(struct fdw_options));
    extract_table_options(foreigntableid, options);

    baserel->fdw_private = options;
}

static void
tupleGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
    double  startup_cost = 0;
    double  total_cost = 100;
    List   *pathkeys = NIL;
    ListCell *lc;
    struct fdw_options *options = (struct fdw_options *) baserel->fdw_private;

    foreach (lc, options->attrs_sorted)
    {
        AttrNumber  attnum = lfirst_int(lc);
        Oid         relid = root->simple_rte_array[baserel->relid]->relid;
        Oid         typid,
                    collid;
        int32       typmod;
        Oid         sort_op;
        Var        *var;
        List       *attr_pathkey;

        /* Build an expression (simple var) */
        get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);
        var = makeVar(baserel->relid, attnum, typid, typmod, collid, 0);

        /* Lookup sorting operator for the attribute type */
        get_sort_group_operators(typid,
                                 true, false, false,
                                 &sort_op, NULL, NULL,
                                 NULL);

        attr_pathkey = build_expression_pathkey(root, (Expr *) var, NULL,
                                                sort_op, baserel->relids,
                                                true);
        pathkeys = list_concat(pathkeys, attr_pathkey);
    }

	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
                                     NULL,	/* default pathtarget */
                                     baserel->rows,
                                     startup_cost,
                                     total_cost,
                                     pathkeys,  /* no info on sorting */
                                     NULL,	/* no outer rel either */
                                     NULL,	/* no extra plan */
                                     NULL));
}

static ForeignScan *
tupleGetForeignPlan(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid,
                      ForeignPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      Plan *outer_plan)
{
    List               *fdw_private = NIL;

    fdw_private = fdw_options_to_list((struct fdw_options *) baserel->fdw_private);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
                            scan_clauses,
                            baserel->relid,
                            NIL,	/* no expressions to evaluate */
                            fdw_private,
                            NIL,	/* no custom tlist */
                            NIL,	/* no remote quals */
                            outer_plan);
}

static void
unmap_file_callback(void *arg)
{
    StorageState *state = (StorageState *) arg;

    unmap_file(state);
}

static void
tupleBeginForeignScan(ForeignScanState *node, int eflags)
{
    StorageState   *state;
    ForeignScan    *plan = (ForeignScan *) node->ss.ps.plan;
    List           *fdw_private = plan->fdw_private;
    char           *filename;
    bool            use_mmap;

    state = palloc0(sizeof(StorageState));

    Assert(list_length(fdw_private) == 3);
    filename = strVal(linitial(fdw_private));
    use_mmap = intVal(lsecond(fdw_private));

    /* open file */
    StorageInit(state, filename, true, use_mmap);

    if (use_mmap)
    {
        EState     *estate = node->ss.ps.state;
        MemoryContextCallback *callback;

        /* unmap files automatically by using memory context callback */
        callback = palloc0(sizeof(MemoryContextCallback));
        callback->func = unmap_file_callback;
        callback->arg = (void *) state;
        MemoryContextRegisterResetCallback(estate->es_query_cxt, callback);
    }

    node->fdw_state = state;
}

static TupleTableSlot *
tupleIterateForeignScan(ForeignScanState *node)
{
    StorageState   *state = (StorageState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    HeapTuple tuple;

	ExecClearTuple(slot);

    if ((tuple = StorageReadTuple(state)) == NULL)
        return slot;

#if PG_VERSION_NUM < 120000
    ExecStoreTuple(tuple, slot, InvalidBuffer, false);
#else
    ExecStoreHeapTuple(tuple, slot, false);
#endif

    return slot;
}

static void
tupleEndForeignScan(ForeignScanState *node)
{
	StorageState *state = (StorageState *) node->fdw_state;

    StorageRelease(state);
}

static List *
tuplePlanForeignModify(PlannerInfo *root,
                       ModifyTable *plan,
                       Index resultRelation,
                       int subplan_index)
{
    struct fdw_options  options;
	RangeTblEntry      *rte = planner_rt_fetch(resultRelation, root);

    extract_table_options(rte->relid, &options);
    return fdw_options_to_list(&options);
}

static void
tupleBeginForeignModify(ModifyTableState *mtstate,
                        ResultRelInfo *resultRelInfo,
                        List *fdw_private,
                        int subplan_index,
                        int eflags)
{
    Relation        rel = resultRelInfo->ri_RelationDesc;
    StorageState   *state = palloc0(sizeof(StorageState));
    char           *filename = strVal(linitial(fdw_private));

    /*
     * Prevent relation from being modified concurrently or being modified and
     * read at the same time. The storage itself doesn't have any internal
     * mechanisms to resolve concurrent access.
     */
    LockRelation(rel, AccessExclusiveLock);

    StorageInit(state, filename, false, false);
    state->lz4_acceleration = intVal(lthird(fdw_private));

	resultRelInfo->ri_FdwState = state;
}

static TupleTableSlot *
tupleExecForeignInsert(EState *estate,
                       ResultRelInfo *resultRelInfo,
                       TupleTableSlot *slot,
                       TupleTableSlot *planSlot)
{
	StorageState  *state = (StorageState *) resultRelInfo->ri_FdwState;
	TupleTableSlot *rslot = slot;
    HeapTuple       tuple;

#if PG_VERSION_NUM < 120000
	tuple = ExecCopySlotTuple(slot);
#else
	tuple = ExecCopySlotHeapTuple(slot);
#endif

    StorageInsertTuple(state, tuple);

    return rslot;
}

static void
tupleEndForeignModify(EState *estate,
                      ResultRelInfo *resultRelInfo)
{
	StorageState *state = (StorageState *) resultRelInfo->ri_FdwState;

    StorageRelease(state);
}
