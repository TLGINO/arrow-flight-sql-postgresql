#!/bin/bash
set -e

ninja -C builddir
sudo cp builddir/arrow_flight_sql.so /home/martin/pgdev/lib/postgresql/
pg_ctl -D /home/martin/pgdata restart
