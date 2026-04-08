import os
import subprocess

import pyarrow as pa
import pyarrow.flight as flight
import pytest

FLIGHT_URI = os.environ.get("FLIGHT_URI", "grpc://127.0.0.1:15432")
PGUSER = os.environ.get("PGUSER", os.environ.get("USER", "postgres"))
PGDATABASE = os.environ.get("PGDATABASE", "postgres")
FLIGHT_PASSWORD = os.environ.get("FLIGHT_PASSWORD", "")


@pytest.fixture
def flight_client():
    client = flight.connect(FLIGHT_URI)
    yield client


@pytest.fixture
def call_options(flight_client):
    token = flight_client.authenticate_basic_token(PGUSER, FLIGHT_PASSWORD)
    opts = flight.FlightCallOptions(headers=[token])
    yield opts
    try:
        action = flight.Action("CloseSession", b"")
        list(flight_client.do_action(action, opts))
    except Exception:
        pass


def _psql(sql, db=None):
    cmd = ["psql", "-d", db or PGDATABASE, "-U", PGUSER, "-Atq", "-c", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr.strip()}")
    return result.stdout.strip()


@pytest.fixture(scope="session")
def psql():
    return _psql


@pytest.fixture(scope="module")
def fdw_tables(psql):
    """Create FDW infrastructure + test tables, tear down after."""
    setup_sql = """
    CREATE OR REPLACE FUNCTION arrow_flight_sql_fdw_handler()
      RETURNS fdw_handler AS 'arrow_flight_sql' LANGUAGE C STRICT;
    CREATE OR REPLACE FUNCTION arrow_flight_sql_fdw_validator(text[], oid)
      RETURNS void AS 'arrow_flight_sql' LANGUAGE C STRICT;

    DROP FOREIGN DATA WRAPPER IF EXISTS arrow_flight_sql CASCADE;
    CREATE FOREIGN DATA WRAPPER arrow_flight_sql
      HANDLER arrow_flight_sql_fdw_handler
      VALIDATOR arrow_flight_sql_fdw_validator;

    CREATE SERVER IF NOT EXISTS loopback
      FOREIGN DATA WRAPPER arrow_flight_sql
      OPTIONS (uri 'grpc://127.0.0.1:15432');

    DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER loopback;
    CREATE USER MAPPING FOR CURRENT_USER SERVER loopback
      OPTIONS (username '{user}', password '');

    DROP TABLE IF EXISTS fdw_test_local CASCADE;
    CREATE TABLE fdw_test_local (
      id integer PRIMARY KEY,
      name text,
      value double precision
    );
    INSERT INTO fdw_test_local VALUES
      (1, 'alice', 10.5),
      (2, 'bob', 20.3),
      (3, 'charlie', 30.7);

    DROP FOREIGN TABLE IF EXISTS fdw_test_remote;
    CREATE FOREIGN TABLE fdw_test_remote (
      id integer,
      name text,
      value double precision
    ) SERVER loopback
      OPTIONS (table_name 'fdw_test_local');

    DROP FOREIGN TABLE IF EXISTS fdw_test_query;
    CREATE FOREIGN TABLE fdw_test_query (
      total_value double precision
    ) SERVER loopback
      OPTIONS (query 'SELECT SUM(value) AS total_value FROM fdw_test_local');
    """.format(user=PGUSER)

    psql(setup_sql)

    # Writable table + foreign table (separate transaction so remote can see it)
    psql("DROP TABLE IF EXISTS fdw_write_target CASCADE")
    psql("""CREATE TABLE fdw_write_target (
              id integer, name text, value double precision)""")
    psql("""DROP FOREIGN TABLE IF EXISTS fdw_write_remote;
            CREATE FOREIGN TABLE fdw_write_remote (
              id integer, name text, value double precision
            ) SERVER loopback
              OPTIONS (table_name 'fdw_write_target')""")
    yield

    teardown_sql = """
    DROP FOREIGN TABLE IF EXISTS fdw_write_remote;
    DROP TABLE IF EXISTS fdw_write_target CASCADE;
    DROP FOREIGN TABLE IF EXISTS fdw_test_query;
    DROP FOREIGN TABLE IF EXISTS fdw_test_remote;
    DROP TABLE IF EXISTS fdw_test_local CASCADE;
    DROP SERVER IF EXISTS loopback CASCADE;
    DROP FOREIGN DATA WRAPPER IF EXISTS arrow_flight_sql CASCADE;
    """
    try:
        psql(teardown_sql)
    except Exception:
        pass
