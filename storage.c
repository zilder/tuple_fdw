#include "postgres.h"
#include "storage/fd.h"

#include "storage.h"

#include <stdio.h>


/*
 * Storage structure
 * -----------------
 *
 * Storage file consists of header and a set of data blocks each of which
 * contains tuples. Data block starts with a 8 byte integer value signifying
 * the compressed block data size. Compressed data consists of (length, tuple)
 * pairs.
 *
 * Storage header currently contains only the last block offset to speedup
 * inserts.
 *
 */


static void allocate_new_block(StorageState *state);


/* Basic low level operations */

static inline void
storage_seek(StorageState *state, Size pos)
{
    if (fseek(state->file, pos, SEEK_SET) != 0)
        elog(ERROR, "fseek failed");
}

static inline void
storage_write(StorageState *state, const void *ptr, Size size)
{
    if (fwrite((char *) ptr, 1, size, state->file) != size)
    {
        const char *err = strerror(errno);

        elog(ERROR, "write failed: %s", err);
    }
}

/* Storage file manipulations */

static void
write_storage_file_header(StorageState *state)
{
    storage_seek(state, 0);
    storage_write(state, &state->file_header, sizeof(StorageFileHeader));
}

static void
read_storage_file_header(StorageState *state)
{
    Size bytes;

    storage_seek(state, 0);
    bytes = fread(&state->file_header, 1, sizeof(StorageFileHeader), state->file);

    if (bytes == 0)
    {   
        /* brand new file, need to write file header first */
        state->file_header.last_block_offset = sizeof(StorageFileHeader);
        write_storage_file_header(state);
    }
}

static void
load_last_block(StorageState *state)
{
    Size    bytes;

    /* read the last block */
    storage_seek(state, state->file_header.last_block_offset);
    bytes = fread(state->last_block.data, 1, BLOCK_SIZE, state->file);
    if (bytes == BLOCK_SIZE)
    {
        state->last_block.offset = state->file_header.last_block_offset;
        state->last_block.status = BS_LOADED;
    }
    else
    {
        char *err = strerror(errno);

        elog(NOTICE, "fread failed: %s", err);
        allocate_new_block(state);
    }
}

static void
find_last_tuple_offset(StorageState *state)
{
    Size    off = 0;

    /* iterate over tuples in the block */
    while (off < BLOCK_SIZE)
    {
        StorageTupleHeader  *st_header;

        st_header = (StorageTupleHeader *) (state->last_block.data + off);

        if (st_header->length == 0)
            break;

        off = off + st_header->length + sizeof(StorageTupleHeader);
    }

    state->last_offset = off;
}

static void
flush_last_block(StorageState *state)
{
    Assert(state->last_block.status != BS_INVALID);

    if (state->last_block.status == BS_LOADED)
    {
        /* no modifications were made */
        return;
    }

    storage_seek(state, state->last_block.offset);
    storage_write(state, state->last_block.data, BLOCK_SIZE);

    /* if new block is being flushed overwrite the file header */
    if (state->last_block.status == BS_NEW)
    {
        state->file_header.last_block_offset = state->last_block.offset;
        write_storage_file_header(state);
        state->last_block.status = BS_LOADED;
    }
}

static void
allocate_new_block(StorageState *state)
{
    Block *block = &state->last_block;

    if (block->offset != 0)
        block->offset = state->last_block.offset + BLOCK_SIZE;
    else
    {
        /*
         * this is the first block in the storage, it goes straight next to
         * the file header
         */
        block->offset = sizeof(StorageFileHeader);
    }

    memset(block->data, 0, BLOCK_SIZE);
    block->status = BS_NEW;

    state->last_offset = 0;
}

void
StorageInit(StorageState *state, const char *filename)
{
    if ((state->file = AllocateFile(filename, "r+")) == NULL)
    {
        const char *err = strerror(errno);
        elog(ERROR, "tuple_fdw: cannot open file '%s': %s", filename, err);
    }
    read_storage_file_header(state);
}

void
StorageInsertTuple(StorageState *state, HeapTuple tuple)
{
    StorageTupleHeader st_header;
    char *buf;

    if (state->last_block.status == BS_INVALID)
    {
        load_last_block(state);
        find_last_tuple_offset(state);
    }

    /* does the tuple fit current block? */
    if (state->last_offset + tuple->t_len + sizeof(StorageTupleHeader) > BLOCK_SIZE)
    {
        /* if not, create new block */
        flush_last_block(state);
        allocate_new_block(state);
    }

    /* write tuple length and the tuple itself to the block */
    st_header.length = tuple->t_len;
    buf = state->last_block.data + state->last_offset;
    memcpy(buf, &st_header, sizeof(StorageTupleHeader));
    memcpy(buf + sizeof(StorageTupleHeader), tuple->t_data, tuple->t_len);

    if (state->last_block.status != BS_NEW)
        state->last_block.status = BS_MODIFIED;
}

void
StorageReadTuple(StorageState *state, HeapTuple *tuple)
{
}

void
StorageRelease(StorageState *state)
{
    BlockStatus status = state->last_block.status;

    /* flush pending block */
    if (status == BS_NEW || status == BS_MODIFIED)
        flush_last_block(state);

    FreeFile(state->file);
}
