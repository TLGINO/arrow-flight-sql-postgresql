#!/bin/bash
set -e

PGLIB=$(pg_config --pkglibdir)
PGDATA="${PGDATA:-$(psql -d postgres -Atc 'SHOW data_directory;' 2>/dev/null || echo "$HOME/pgdata")}"

ninja -C builddir
cp builddir/arrow_flight_sql.so "$PGLIB/"
pg_ctl -D "$PGDATA" restart -m immediate
