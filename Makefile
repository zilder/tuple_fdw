MODULE_big = tuple_fdw
OBJS = storage.o tuple_fdw.o 
PGFILEDESC = "tuple_fdw - foreign data wrapper for tuple"

SHLIB_LINK = -llz4

EXTENSION = tuple_fdw
DATA = tuple_fdw--0.1.sql

REGRESS = tuple_fdw

REGRESSION_DATA = sql/example.bin
EXTRA_CLEAN = sql/tuple_fdw.sql expected/tuple_fdw.out $(REGRESSION_DATA)

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

installcheck: cleandata

cleandata:
	rm -f $(REGRESSION_DATA)
