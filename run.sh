#!/bin/bash
set -e

PGLIB=$(pg_config --pkglibdir)

ninja -C builddir
cp builddir/arrow_flight_sql.so "$PGLIB/"
pg_ctl -D "$PGDATA" restart -m immediate
