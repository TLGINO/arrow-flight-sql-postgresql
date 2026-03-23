#!/bin/bash
set -e

ninja -C builddir
cp builddir/arrow_flight_sql.so /home/martin/pgdev/lib/postgresql/
AFS_EXPLAIN=${AFS_EXPLAIN:-analyze} pg_ctl -D /home/martin/pgdata restart -m immediate
