#include <errno.h>
#include <sys/stat.h>

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "utils/elog.h"


PG_MODULE_MAGIC;

#define ELOG_PREFIX "tuple_fdw: "

typedef struct TupleFdwState
{
    FILE *file;
} TupleFdwState;


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
            struct stat stat_buf;
            const char *filename = defGetString(def);

            if (stat(filename, &stat_buf) != 0)
            {
                const char *err = strerror(errno);

                ereport(ERROR,
                        (errmsg(ELOG_PREFIX "cannot get file status for '%s': %s",
                                filename, err)));
            }
            filename_provided = true;
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
tupleGetForeignRelSize(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid foreigntableid)
{
}

static void
tupleGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
    double startup_cost = 0;
    double total_cost = 100;

	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
                                     NULL,	/* default pathtarget */
                                     baserel->rows,
                                     startup_cost,
                                     total_cost,
                                     NULL,  /* no info on sorting */
                                     NULL,	/* no outer rel either */
                                     NULL,	/* no extra plan */
                                     NULL));
}

static List *
extract_table_options(Oid relid)
{
	ForeignTable   *table;
    List           *fdw_private = NIL;
    ListCell       *lc;

    table = GetForeignTable(relid);

    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
            fdw_private = lappend(fdw_private, def->arg);
    }

    return fdw_private;
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
    List       *fdw_private;

    fdw_private = extract_table_options(foreigntableid);

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
tupleBeginForeignScan(ForeignScanState *node, int eflags)
{
    TupleFdwState  *tstate;
	ForeignScan    *plan = (ForeignScan *) node->ss.ps.plan;
    List           *fdw_private = plan->fdw_private;
    char           *filename;

    tstate = palloc(sizeof(TupleFdwState));

    Assert(list_length(fdw_private) == 1);
    filename = strVal(linitial(fdw_private));

    /* open file */
    if ((tstate->file = AllocateFile(filename, PG_BINARY_R)) == NULL)
    {
        const char *err = strerror(errno);
        elog(ERROR, "tuple_fdw: cannot open file '%s': %s", filename, err);
    }

    node->fdw_state = tstate;
}

static TupleTableSlot *
tupleIterateForeignScan(ForeignScanState *node)
{
	TupleFdwState *tstate = (TupleFdwState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    int64    len = 0;
    char   *data;
    HeapTuple tuple;

	ExecClearTuple(slot);

    /* read tuple data size */
    if (fread(&len, 8, 1, tstate->file) == 0)
    {
        /*TODO: check for errors */
        return slot;
    }

    /* read data */
    data = palloc(len);
    fread(data, len, 1, tstate->file);

    tuple = palloc(sizeof(HeapTupleData));
    tuple->t_len = len;
    tuple->t_data = (HeapTupleHeader) data;

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
	TupleFdwState *tstate = (TupleFdwState *) node->fdw_state;

    FreeFile(tstate->file);
}

static List *
tuplePlanForeignModify(PlannerInfo *root,
                       ModifyTable *plan,
                       Index resultRelation,
                       int subplan_index)
{
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);

    return extract_table_options(rte->relid);
}

static void
tupleBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
    TupleFdwState *tstate = palloc(sizeof(TupleFdwState));
    char *filename = strVal(linitial(fdw_private));

    /* open file */
    if ((tstate->file = AllocateFile(filename, PG_BINARY_A)) == NULL)
    {
        const char *err = strerror(errno);
        elog(ERROR, "tuple_fdw: cannot open file '%s': %s", filename, err);
    }

	resultRelInfo->ri_FdwState = tstate;
}

static TupleTableSlot *
tupleExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	TupleFdwState  *tstate = (TupleFdwState *) resultRelInfo->ri_FdwState;
	TupleTableSlot *rslot = slot;
    HeapTuple       tuple;
    char           *buf;

#if PG_VERSION_NUM < 120000
	tuple = ExecCopySlotTuple(slot);
#else
	tuple = ExecCopySlotHeapTuple(slot);
#endif

    /* TODO */
    buf = palloc0(8 + tuple->t_len);
    memcpy(buf, (char *) &tuple->t_len, 4);
    memcpy(buf + 8, (char *) tuple->t_data, tuple->t_len);

    fwrite(buf, 1, tuple->t_len + 8, tstate->file);
    pfree(buf);

    return rslot;
}

static void
tupleEndForeignModify(EState *estate,
                      ResultRelInfo *resultRelInfo)
{
	TupleFdwState *tstate = (TupleFdwState *) resultRelInfo->ri_FdwState;

    FreeFile(tstate->file);
}
