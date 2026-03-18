#!/usr/bin/env python3
"""
Long-running Flight SQL helper: reads plan paths from stdin, writes Arrow IPC
results. Reuses a single Flight SQL client/session across all queries.
Reconnects automatically on auth errors.

Protocol (line-based on stdin/stdout):
  IN:  <plan_path> <out_path>        # execute Substrait plan
  IN:  SQL <sql_path> <out_path>     # execute SQL query
  OUT: OK
  OUT: ERR <message>
  IN:  (empty line or EOF) → exit
"""
import sys
import traceback

import os

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


def main():
    helper = FlightHelper()
    helper.connect()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            break

        try:
            parts = line.split(None, 2)
            if parts[0] == "SQL":
                sql_path, out_path = parts[1], parts[2]
                with open(sql_path, "r") as f:
                    query = f.read()
                try:
                    table = helper.execute_sql(query)
                except flight.FlightUnauthorizedError:
                    helper.connect()
                    table = helper.execute_sql(query)
            else:
                plan_path, out_path = parts[0], parts[1]
                with open(plan_path, "rb") as f:
                    plan_bytes = f.read()
                try:
                    table = helper.execute(plan_bytes)
                except flight.FlightUnauthorizedError:
                    helper.connect()
                    table = helper.execute(plan_bytes)

            writer = ipc.new_file(out_path, table.schema)
            writer.write_table(table)
            writer.close()

            print("OK", flush=True)
        except Exception:
            msg = traceback.format_exc().strip().split("\n")[-1]
            print(f"ERR {msg}", flush=True)

    if helper.client:
        helper.client.close()


if __name__ == "__main__":
    main()
