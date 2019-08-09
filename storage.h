#ifndef TUPLE_STORAGE_H
#define TUPLE_STORAGE_H

#include "access/htup.h"
#include "port/pg_crc32c.h"


#define BLOCK_SIZE 1024 * 1024  /* 1 megabyte */

typedef enum
{
    BS_INVALID,
    BS_NEW,
    BS_LOADED,
    BS_MODIFIED
} BlockStatus;

#define BlockIsInvalid(block) ((block).status == BS_INVALID)


typedef struct
{
    Size    last_block_offset;
    /* TODO: compression type */
    /* TODO: block size & version */
} StorageFileHeader;


typedef struct
{
    int32_t     compressed_size;
    pg_crc32c   checksum;
    /* TODO: store the last tuple offset */
    char        data[];     /* compressed block data */
} StorageBlockHeader;

#define StorageBlockHeaderSize offsetof(StorageBlockHeader, data)


typedef struct
{
    Size    length;
    char    data[];     /* tuple body */
} StorageTupleHeader;

#define StorageTupleHeaderSize offsetof(StorageTupleHeader, data)
#define GetCurrentTuple(state) \
    (StorageTupleHeader *) ((state)->cur_block.data + (state)->cur_offset)


typedef struct
{
    BlockStatus status;
    Size        offset;
    Size        compressed_size;
    char        data[BLOCK_SIZE];
} Block;


typedef struct
{
    /* TODO: add exclusive write lock */
    FILE       *file;
    char       *mmaped_file;    /* address of mmaped segment */
    Size        mmaped_size;    /* size of mmaped segment */
    bool        readonly;
    StorageFileHeader    file_header;
    Block       cur_block;
    Size        cur_offset;    /* offset within the last_block */
} StorageState;


void StorageInit(StorageState *state,
            const char *filename,
            bool readonly,
            bool use_mmap);
void StorageInsertTuple(StorageState *state, HeapTuple tuple);
HeapTuple StorageReadTuple(StorageState *state);
void StorageRelease(StorageState *state);
void unmap_file(StorageState *state);

#endif /* TUPLE_STORAGE_H */
