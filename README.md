# tuple_fdw

A very simple foreign data wrapper to write/read postgres tuples to/from binary file. Unlike postgres heap storage this is just a plain sequence of tuples. `tuple_fdw` writes (and reads) directly to the file avoiding postgres buffer cache. It isn't affected by autovacuum. The storage is append only.

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

The only configurable parameter is the `filename` table option which specifies the storage file. `tuple_fdw` doesn't automatically create file if it doesn't exist, so please create it manually.

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
