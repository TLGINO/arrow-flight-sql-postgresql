#!/usr/bin/env python3
"""
Long-running Flight SQL helper: reads plan/SQL from stdin, writes Arrow IPC
results to stdout. Reuses a single ADBC Flight SQL connection across all queries.
Reconnects automatically on errors.

Protocol (binary on stdin/stdout):
  IN:  b"PLAN <nbytes>\n" + <plan_bytes>     # execute Substrait plan
  IN:  b"SQL <nbytes>\n"  + <sql_bytes>      # execute SQL query
  OUT: b"OK <nbytes>\n"   + <arrow_ipc>      # Arrow IPC stream
  OUT: b"ERR <message>\n"                     # error
  IN:  b"\n" or EOF → exit
"""
import os
import sys
import traceback

import pyarrow as pa
import pyarrow.ipc as ipc

import adbc_driver_flightsql.dbapi as flightsql

FLIGHT_URI = os.environ.get("FLIGHT_URI", "grpc://127.0.0.1:15432")
FLIGHT_USER = os.environ.get("PGUSER", os.environ.get("USER", "postgres"))
FLIGHT_PASSWORD = os.environ.get("FLIGHT_PASSWORD", "")
FLIGHT_DATABASE = os.environ.get("PGDATABASE", "postgres")


class FlightHelper:
    def __init__(self):
        self.conn = None

    def connect(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = flightsql.connect(
            FLIGHT_URI,
            db_kwargs={
                "username": FLIGHT_USER,
                "password": FLIGHT_PASSWORD,
                "adbc.flight.sql.rpc.call_header.x-flight-sql-database": FLIGHT_DATABASE,
            },
        )

    def execute(self, plan_bytes):
        with self.conn.cursor() as cur:
            cur.adbc_statement.set_substrait_plan(plan_bytes)
            handle, _ = cur.adbc_statement.execute_query()
            reader = pa.RecordBatchReader.from_stream(handle)
            return reader.read_all()

    def execute_sql(self, query):
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetch_arrow_table()


def read_exact(stream, n):
    """Read exactly n bytes from a binary stream."""
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def table_to_ipc_bytes(table):
    """Serialize an Arrow table to IPC stream bytes."""
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def main():
    helper = FlightHelper()
    helper.connect()

    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        line = stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            break

        try:
            text = line.decode("utf-8")
            if text.startswith("PLAN "):
                nbytes = int(text[5:])
                plan_bytes = read_exact(stdin, nbytes)
                try:
                    table = helper.execute(plan_bytes)
                except Exception:
                    helper.connect()
                    table = helper.execute(plan_bytes)
            elif text.startswith("SQL "):
                nbytes = int(text[4:])
                sql = read_exact(stdin, nbytes).decode("utf-8")
                try:
                    table = helper.execute_sql(sql)
                except Exception:
                    helper.connect()
                    table = helper.execute_sql(sql)
            else:
                stdout.write(b"ERR unknown command\n")
                stdout.flush()
                continue

            ipc_bytes = table_to_ipc_bytes(table)
            stdout.write(f"OK {len(ipc_bytes)}\n".encode())
            stdout.write(ipc_bytes)
            stdout.flush()
        except Exception:
            msg = traceback.format_exc().strip().split("\n")[-1]
            stdout.write(f"ERR {msg}\n".encode())
            stdout.flush()
            helper.connect()  # fresh session for next query

    if helper.conn:
        helper.conn.close()


if __name__ == "__main__":
    main()
