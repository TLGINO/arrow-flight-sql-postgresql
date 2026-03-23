#!/bin/bash
set -e

ninja -C builddir
cp builddir/arrow_flight_sql.so /home/martin/pgdev/lib/postgresql/
pg_ctl -D /home/martin/pgdata restart -m immediate
