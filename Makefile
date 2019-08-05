MODULE_big = tuple_fdw
OBJS = storage.o tuple_fdw.o 
PGFILEDESC = "tuple_fdw - foreign data wrapper for tuple"

SHLIB_LINK = -llz4

EXTENSION = tuple_fdw
DATA = tuple_fdw--0.1.sql

REGRESS = tuple_fdw

EXTRA_CLEAN = sql/tuple_fdw.sql expected/tuple_fdw.out

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

#tuple.bc:
#	$(COMPILE.cxx.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ tuple_impl.cpp

#tuple.o:
#	$(CXX) -std=c++11 -O3 $(CPPFLAGS) $(CCFLAGS) tuple_impl.cpp $(PG_LIBS) -c -fPIC $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

