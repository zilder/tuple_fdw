# tuple_fdw

A very simple foreign data wrapper to write/read postgres tuples to/from binary file. The data stored in a file is organized as blocks, each block is compressed using `lz4`. `tuple_fdw` writes (and reads) directly to the file avoiding postgres buffer cache. It isn't affected by autovacuum. The storage is append only.

Because of the nature of the storage it doesn't support concurrent writes or mixture of concurrent reads and writes. Also it's not well suited for single row insertions as it has to decompress and compress the last data block to perform insertion. The storage is best suited as a cold data storage.

Some visualization of the internals of the storage can be found in `storage.c`.

## Dependencies

`tuple_fdw` requires `liblz4` installed.

## Build

Like with any other postgres extensions after clone just run in command line:

```
make install
```

Or if postgres binaries are not in the `PATH`:

```
make install PG_CONFIG=/path/to/pg_config
```

## Using


When creating foreign table using `tuple_fdw` the following options are avaliable:

* `filename`: path to the storage file; if file doesn't exist it will be created automatically;
* `use_mmap`: use `mmap` for reading data rather than `fread`; in heavy concurrent read workload it might be more efficient to use mmap;
* `sorted` specifies columns by which the dataset is ordered; it may help building more efficient execution plans which imply ordering;
* `lz4_acceleration`: specific `lz4` parameter responsible for performance; the higher value the faster compression/decompression and the lower compression ratio.

## Example

```sql
create server tuple_srv foreign data wrapper tuple_fdw;
create user mapping for postgres server tuple_srv options (user 'postgres');
create foreign table my_table (
    a int,
    b int
)
server tuple_srv
options (filename '/tmp/my_table.bin');
```
