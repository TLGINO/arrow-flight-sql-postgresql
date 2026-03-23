#!/usr/bin/env python3
"""
Long-running Flight SQL helper: reads plan/SQL from stdin, writes Arrow IPC
results to stdout. Reuses a single Flight SQL client/session across all queries.
Reconnects automatically on auth errors.

Protocol (binary on stdin/stdout):
  IN:  b"PLAN <nbytes>\n" + <plan_bytes>     # execute Substrait plan
  IN:  b"SQL <nbytes>\n"  + <sql_bytes>      # execute SQL query
  OUT: b"OK <nbytes>\n"   + <arrow_ipc>      # Arrow IPC stream
  OUT: b"ERR <message>\n"                     # error
  IN:  b"\n" or EOF → exit
"""
import sys
import traceback

import os

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc

FLIGHT_URI = os.environ.get("FLIGHT_URI", "grpc://127.0.0.1:15432")
FLIGHT_USER = os.environ.get("PGUSER", os.environ.get("USER", "postgres"))
FLIGHT_PASSWORD = os.environ.get("FLIGHT_PASSWORD", "")


def varint(v):
    r = []
    while v > 0x7F:
        r.append((v & 0x7F) | 0x80)
        v >>= 7
    r.append(v & 0x7F)
    return bytes(r)


def pb_field(n, data):
    return varint((n << 3) | 2) + varint(len(data)) + data


class FlightHelper:
    def __init__(self):
        self.client = None
        self.options = None

    def connect(self):
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
        self.client = flight.FlightClient(FLIGHT_URI)
        token_pair = self.client.authenticate_basic_token(FLIGHT_USER, FLIGHT_PASSWORD)
        self.options = flight.FlightCallOptions(headers=[token_pair])

    def execute(self, plan_bytes):
        cmd = pb_field(
            1,
            b"type.googleapis.com/arrow.flight.protocol.sql"
            b".CommandStatementSubstraitPlan",
        ) + pb_field(2, pb_field(1, pb_field(1, plan_bytes)))

        info = self.client.get_flight_info(
            flight.FlightDescriptor.for_command(cmd), self.options
        )
        reader = self.client.do_get(info.endpoints[0].ticket, self.options)
        return reader.read_all()

    def execute_sql(self, query):
        cmd = pb_field(
            1,
            b"type.googleapis.com/arrow.flight.protocol.sql"
            b".CommandStatementQuery",
        ) + pb_field(2, pb_field(1, query.encode("utf-8")))

        info = self.client.get_flight_info(
            flight.FlightDescriptor.for_command(cmd), self.options
        )
        reader = self.client.do_get(info.endpoints[0].ticket, self.options)
        return reader.read_all()


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
                except flight.FlightUnauthorizedError:
                    helper.connect()
                    table = helper.execute(plan_bytes)
            elif text.startswith("SQL "):
                nbytes = int(text[4:])
                sql = read_exact(stdin, nbytes).decode("utf-8")
                try:
                    table = helper.execute_sql(sql)
                except flight.FlightUnauthorizedError:
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

    if helper.client:
        helper.client.close()


if __name__ == "__main__":
    main()
