#include "postgres.h"
#include "storage/fd.h"
#include "lz4.h"

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

static inline void
storage_read(StorageState *state, const void *ptr, Size size)
{
    if (fread((char *) ptr, 1, size, state->file) != size)
    {
        const char *err = strerror(errno);

        elog(ERROR, "read failed: %s", err);
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
        /* it's a brand new file, initialize new header */
        state->file_header.last_block_offset = sizeof(StorageFileHeader);

        /* write it to the disk if possible*/
        if (!state->readonly)
            write_storage_file_header(state);
    }
}

static void
decompress_block(StorageState *state, char *compressed_data, Size len)
{
    Size size;

    size = LZ4_decompress_safe(compressed_data,
                               state->cur_block.data,
                               len,
                               BLOCK_SIZE);
    if (size < 0)
        elog(ERROR, "tuple_fdw: decompression failed");

    Assert(BLOCK_SIZE == size);
}

static bool
read_block(StorageState* state, Size offset)
{
    StorageBlockHeader block_header;
    char   *compressed_data;
    Size    bytes;

    /* read the block */
    storage_seek(state, offset);

    bytes = fread(&block_header, 1, StorageBlockHeaderSize, state->file);
    if (bytes != StorageBlockHeaderSize)
        return false;

    Assert(block_header.compressed_size > 0);

    compressed_data = palloc(block_header.compressed_size);
    bytes = fread(compressed_data, 1, block_header.compressed_size, state->file);
    if (bytes == block_header.compressed_size)
    {
        decompress_block(state, compressed_data, block_header.compressed_size);

        state->cur_block.offset = offset;
        state->cur_block.status = BS_LOADED;
        state->cur_block.compressed_size = block_header.compressed_size;
        state->cur_offset = 0;
        return true;
    }
    else
        return false;

}

static bool
load_next_block(StorageState *state)
{
    Size    offset;

    if (BlockIsInvalid(state->cur_block))
    {
        /* we're about to read the first block in the file */
        offset = sizeof(StorageFileHeader);
    }
    else
    {
        offset = state->cur_block.offset \
            + StorageBlockHeaderSize \
            + state->cur_block.compressed_size;
    }

    return read_block(state, offset);
}

static void
load_last_block(StorageState *state)
{
    /* read the last block */
    if (read_block(state, state->file_header.last_block_offset) == true)
    {
        state->cur_block.offset = state->file_header.last_block_offset;
        state->cur_block.status = BS_LOADED;
    }
    else
    {
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

        st_header = (StorageTupleHeader *) (state->cur_block.data + off);

        if (st_header->length == 0)
            break;

        off = off + st_header->length + StorageTupleHeaderSize;
    }

    state->cur_offset = off;
}

static StorageBlockHeader *
compress_current_block(StorageState *state)
{
    Size    estimate;
    Size    size;
    StorageBlockHeader *block_header;

    estimate = LZ4_compressBound(BLOCK_SIZE);
    block_header = (StorageBlockHeader *) \
        palloc0(StorageBlockHeaderSize + estimate);

    size = LZ4_compress_fast(state->cur_block.data,
                             block_header->data,
                             BLOCK_SIZE,
                             estimate,
                             1); /* TODO: configurable? */
    if (size == 0)
        elog(ERROR, "tuple_fdw: compression failed");
    block_header->compressed_size = size;

    return block_header;
}

static void
flush_last_block(StorageState *state)
{
    StorageBlockHeader *block_header;
    Size                block_size;

    Assert(!BlockIsInvalid(state->cur_block));

    if (state->cur_block.status == BS_LOADED)
    {
        /* no modifications were made */
        return;
    }

    /* compress */
    block_header = compress_current_block(state);
    block_size = StorageBlockHeaderSize + block_header->compressed_size;

    /* write out to disk */
    storage_seek(state, state->cur_block.offset);
    storage_write(state, block_header, block_size);

    state->cur_block.compressed_size = block_header->compressed_size;

    /* if new block is being flushed overwrite the file header */
    if (state->cur_block.status == BS_NEW)
    {
        state->file_header.last_block_offset = state->cur_block.offset;
        write_storage_file_header(state);
        state->cur_block.status = BS_LOADED;
    }

    pfree(block_header);
}

static void
allocate_new_block(StorageState *state)
{
    Block *block = &state->cur_block;

    if (block->offset != 0)
    {
        block->offset = state->cur_block.offset \
                        + StorageBlockHeaderSize \
                        + block->compressed_size;
    }
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

    state->cur_offset = 0;
}

void
StorageInit(StorageState *state, const char *filename, bool readonly)
{
    const char *mode = readonly ? "r" : "r+";

    state->readonly = readonly;
    if ((state->file = AllocateFile(filename, mode)) == NULL)
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
    Size tuple_length = tuple->t_len + StorageTupleHeaderSize;

    if (BlockIsInvalid(state->cur_block))
    {
        load_last_block(state);
        find_last_tuple_offset(state);
    }

    /* does the tuple fit current block? */
    if (state->cur_offset + tuple_length > BLOCK_SIZE)
    {
        /* if not, create new block */
        flush_last_block(state);
        allocate_new_block(state);
    }

    /* write tuple length and the tuple itself to the block */
    st_header.length = tuple->t_len;
    buf = state->cur_block.data + state->cur_offset;
    memcpy(buf, &st_header, StorageTupleHeaderSize);
    memcpy(buf + StorageTupleHeaderSize, tuple->t_data, tuple->t_len);

    if (state->cur_block.status != BS_NEW)
        state->cur_block.status = BS_MODIFIED;

    /* advance the current offset */
    state->cur_offset += tuple_length;
}

HeapTuple
StorageReadTuple(StorageState *state)
{
    StorageTupleHeader *st_header = GetCurrentTuple(state);
    HeapTuple   tuple;

    /* TODO: cur_offset + sizeof(StorageTupleHeader) actually */
    if (BlockIsInvalid(state->cur_block) || state->cur_offset > BLOCK_SIZE
        || st_header->length == 0)
    {
        if (!load_next_block(state))
            return NULL;

        /* read the first tuple in the block */
        st_header = GetCurrentTuple(state);
        if (st_header->length == 0)
            return NULL;
    }

    tuple = palloc0(sizeof(HeapTupleData));
    tuple->t_len = st_header->length;
    tuple->t_data = (HeapTupleHeader) st_header->data;

    state->cur_offset += st_header->length + StorageTupleHeaderSize;

    return tuple;
}

void
StorageRelease(StorageState *state)
{
    BlockStatus status = state->cur_block.status;

    /* flush pending block */
    if (status == BS_NEW || status == BS_MODIFIED)
        flush_last_block(state);

    FreeFile(state->file);
}
