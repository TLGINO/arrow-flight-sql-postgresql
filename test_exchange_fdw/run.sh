#!/bin/bash
set -e

python3 -m pytest test_exchange_fdw/ -v "$@"
