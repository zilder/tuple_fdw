#include "postgres.h"
#include "storage/fd.h"
#include "lz4.h"

#include "storage.h"

#include <stdio.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>


/*
 * Storage structure
 * -----------------
 *
 * Storage file consists of header and a set of data blocks each of which
 * contains tuples. Data block starts with a header containing compressed block
 * data size and checksum. Compressed data consists of tuples, each contains
 * a header and tuple itself (memcpy of HeapTupleHeaderData and tuple body).
 *
 * Storage header currently contains only the last block offset to speedup
 * inserts.
 *
 * The storage file layout can be visualized as follows:
 *
 * ┌──────────────────────────────────────────────┐
 * │ StorageFileHeader                            │   ─ 8 bytes
 * ├──────────────────────────────────────────────┤
 * │ StorageBlockHeader                           │   ─ 8 bytes
 * ├────────────────────┬─────────────────────────┤
 * │ StorageTupleHeader │ tuple body              │  ┐
 * ├──────────┬─────────┴──────────┬──────────────┤  │
 * │          │ StorageTupleHeader │ tuple body   │  │
 * ├──────────┴────────────┬───────┴──────────────┤  │
 * │                       │ StorageTupleHeader   │  │  Compressed data
 * ├───────────────────────┴──────────┬───────────┤  ├─ representing a 1 Mb
 * │ tuple body                       │░░░░░░░░░░░│  │  uncompressed block
 * ├──────────────────────────────────┘░░░░░░░░░░░│  │
 * │░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│  │
 * │░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│  │
 * │░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│  ┘
 * └──────────────────────────────────────────────┘
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

    if (state->mmaped_file)
    {
        Assert(state->readonly);
        memcpy(&state->file_header, state->mmaped_file, sizeof(StorageFileHeader));
    }
    else
    {
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
}

static void
decompress_block(StorageState *state, char *compressed_data, Size len)
{
    int size;

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
    StorageBlockHeader *block_header;
    StorageBlockHeader  b;
    char       *compressed_data;
    Size        bytes;
    pg_crc32c   crc;

    if (state->mmaped_file)
    {
        if (offset >= state->mmaped_size)
            return false;

        block_header = (StorageBlockHeader *) (state->mmaped_file + offset);
        compressed_data = block_header->data;
    }
    else
    {
        /* read the block */
        storage_seek(state, offset);

        bytes = fread(&b, 1, StorageBlockHeaderSize, state->file);
        if (bytes != StorageBlockHeaderSize)
            return false;

        Assert(b.compressed_size > 0);

        compressed_data = palloc(b.compressed_size);
        bytes = fread(compressed_data, 1, b.compressed_size, state->file);
        if (bytes != b.compressed_size)
            return false;

        block_header = &b;
    }

    /* calculate checksum and compare it to a stored one */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, compressed_data, block_header->compressed_size);
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, block_header->checksum))
        elog(ERROR, "tuple_fdw: wrong checksum");

    decompress_block(state, compressed_data, block_header->compressed_size);

    state->cur_block.offset = offset;
    state->cur_block.status = BS_LOADED;
    state->cur_block.compressed_size = block_header->compressed_size;
    state->cur_offset = 0;

    return true;
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
    pg_crc32c   crc;

    estimate = LZ4_compressBound(BLOCK_SIZE);
    block_header = (StorageBlockHeader *) \
        palloc0(StorageBlockHeaderSize + estimate);

    size = LZ4_compress_fast(state->cur_block.data,
                             block_header->data,
                             BLOCK_SIZE,
                             estimate,
                             state->lz4_acceleration);
    if (size == 0)
        elog(ERROR, "tuple_fdw: compression failed");
    block_header->compressed_size = size;

    /* calculate checksum */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, block_header->data, size);
    FIN_CRC32C(crc);
    block_header->checksum = crc;

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
    fsync(fileno(state->file));

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

static void
mmap_file(StorageState *state)
{
    struct stat buf;
    int         fd = fileno(state->file);
    
    if (fstat(fd, &buf) != 0)
    {
        const char *err = strerror(errno);

        elog(ERROR, "tuple_fdw: cannot get file status: %s", err);
    }

    state->mmaped_file = mmap(NULL, buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (state->mmaped_file == MAP_FAILED)
    {
        const char *err = strerror(errno);

        elog(ERROR, "tuple_fdw: mmap failed: %s", err);
    }
    state->mmaped_size = buf.st_size;
}

void
unmap_file(StorageState *state)
{
    if (munmap(state->mmaped_file, state->mmaped_size) == -1)
    {
        const char *err = strerror(errno);

        elog(ERROR, "tuple_fdw: munmap failed: %s", err);
    }
}

void
StorageInit(StorageState *state,
            const char *filename,
            bool readonly,
            bool use_mmap)
{
    const char *mode = readonly ? "r" : "r+";

    /*TODO: assert that use_mmap isn't used in non-readonly queries */
    state->readonly = readonly;
    if ((state->file = AllocateFile(filename, mode)) == NULL)
    {
        const char *err = strerror(errno);
        elog(ERROR, "tuple_fdw: cannot open file '%s': %s", filename, err);
    }

    if (use_mmap)
        mmap_file(state);

    read_storage_file_header(state);
}

void
StorageInsertTuple(StorageState *state, HeapTuple tuple)
{
    StorageTupleHeader st_header;
    char *buf;
    Size tuple_length = MAXALIGN(tuple->t_len + StorageTupleHeaderSize);

    if (tuple_length > BLOCK_SIZE)
        elog(ERROR, "tuple_fdw: maximum tuple size exceeded");

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
    st_header.length = MAXALIGN(tuple->t_len);
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

    /*
     * mmaped file (if any) will automatically be unmmaped due to callback (see
     * `unmap_file_callback`)
     */

    FreeFile(state->file);
}
