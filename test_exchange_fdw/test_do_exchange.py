"""DoExchange RPC tests using pyarrow.flight (raw Flight API)."""
import pyarrow as pa
import pyarrow.flight as flight


def _do_exchange(client, options, query, schema, batches=None):
    """Helper: send batches via DoExchange, return result table."""
    descriptor = flight.FlightDescriptor.for_command(query)
    writer, reader = client.do_exchange(descriptor, options)
    writer.begin(schema)
    if batches:
        for batch in batches:
            writer.write_batch(batch)
    writer.done_writing()

    chunks = []
    for chunk in reader:
        chunks.append(chunk.data)
    writer.close()

    if not chunks:
        return pa.table([])
    return pa.Table.from_batches(chunks)


def test_exchange_with_data(flight_client, call_options):
    """Send 3 rows, filter with WHERE, expect 2 rows back."""
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("value", pa.float64()),
    ])
    batch = pa.record_batch([
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(["alice", "bob", "charlie"], type=pa.string()),
        pa.array([10.5, 20.3, 30.7], type=pa.float64()),
    ], schema=schema)

    query = "SELECT name, value * 2 AS doubled FROM _exchange_input WHERE value > 15"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.num_rows == 2
    names = result.column("name").to_pylist()
    assert "bob" in names
    assert "charlie" in names
    doubled = result.column("doubled").to_pylist()
    assert abs(doubled[names.index("bob")] - 40.6) < 0.01
    assert abs(doubled[names.index("charlie")] - 61.4) < 0.01


def test_exchange_no_data(flight_client, call_options):
    """Plain SQL query via DoExchange, no input data."""
    schema = pa.schema([])
    query = "SELECT 42 AS answer, 'hello' AS greeting"
    result = _do_exchange(flight_client, call_options, query, schema)

    assert result.num_rows == 1
    assert result.column("answer").to_pylist() == [42]
    assert result.column("greeting").to_pylist() == ["hello"]


def test_exchange_empty_batch(flight_client, call_options):
    """Schema with columns but 0 rows — query should return empty."""
    schema = pa.schema([pa.field("x", pa.int32())])
    batch = pa.record_batch([pa.array([], type=pa.int32())], schema=schema)

    query = "SELECT COUNT(*) AS cnt FROM _exchange_input"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.num_rows == 1
    assert result.column("cnt").to_pylist() == [0]


def test_exchange_types(flight_client, call_options):
    """Verify int, float, string, bool columns round-trip correctly."""
    schema = pa.schema([
        pa.field("i", pa.int64()),
        pa.field("f", pa.float64()),
        pa.field("s", pa.string()),
        pa.field("b", pa.bool_()),
    ])
    batch = pa.record_batch([
        pa.array([100, -1], type=pa.int64()),
        pa.array([3.14, -0.001], type=pa.float64()),
        pa.array(["hello", "world"], type=pa.string()),
        pa.array([True, False], type=pa.bool_()),
    ], schema=schema)

    query = "SELECT * FROM _exchange_input ORDER BY i DESC"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.num_rows == 2
    assert result.column("i").to_pylist() == [100, -1]
    assert result.column("s").to_pylist() == ["hello", "world"]
    assert result.column("b").to_pylist() == [True, False]


def test_exchange_large(flight_client, call_options):
    """Send 10K rows, verify count."""
    n = 10000
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("val", pa.float64()),
    ])
    batch = pa.record_batch([
        pa.array(list(range(n)), type=pa.int32()),
        pa.array([float(i) for i in range(n)], type=pa.float64()),
    ], schema=schema)

    query = "SELECT COUNT(*) AS cnt, SUM(val) AS total FROM _exchange_input"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.num_rows == 1
    assert result.column("cnt").to_pylist() == [n]
    expected_sum = float(n * (n - 1) // 2)
    assert abs(result.column("total").to_pylist()[0] - expected_sum) < 0.01


def test_exchange_aggregation(flight_client, call_options):
    """Send rows, aggregate with GROUP BY."""
    schema = pa.schema([
        pa.field("category", pa.string()),
        pa.field("amount", pa.float64()),
    ])
    batch = pa.record_batch([
        pa.array(["a", "b", "a", "b", "a"], type=pa.string()),
        pa.array([10.0, 20.0, 30.0, 40.0, 50.0], type=pa.float64()),
    ], schema=schema)

    query = ("SELECT category, SUM(amount) AS total "
             "FROM _exchange_input GROUP BY category ORDER BY category")
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.num_rows == 2
    cats = result.column("category").to_pylist()
    totals = result.column("total").to_pylist()
    assert cats == ["a", "b"]
    assert abs(totals[0] - 90.0) < 0.01  # 10+30+50
    assert abs(totals[1] - 60.0) < 0.01  # 20+40


# ── NULL handling ──

def test_exchange_nulls(flight_client, call_options):
    """NULLs in input should survive round-trip."""
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("val", pa.float64()),
    ])
    batch = pa.record_batch([
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(["a", None, "c"], type=pa.string()),
        pa.array([1.0, None, 3.0], type=pa.float64()),
    ], schema=schema)

    query = "SELECT * FROM _exchange_input ORDER BY id"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.num_rows == 3
    assert result.column("name").to_pylist() == ["a", None, "c"]
    assert result.column("val").to_pylist()[1] is None


def test_exchange_all_nulls(flight_client, call_options):
    """A column of all NULLs should work."""
    schema = pa.schema([
        pa.field("x", pa.int32()),
        pa.field("y", pa.string()),
    ])
    batch = pa.record_batch([
        pa.array([None, None], type=pa.int32()),
        pa.array([None, None], type=pa.string()),
    ], schema=schema)

    query = "SELECT COUNT(*) AS cnt, COUNT(x) AS non_null FROM _exchange_input"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.column("cnt").to_pylist() == [2]
    assert result.column("non_null").to_pylist() == [0]


# ── Type coverage ──

def test_exchange_smallint(flight_client, call_options):
    """INT16 round-trip."""
    schema = pa.schema([pa.field("v", pa.int16())])
    batch = pa.record_batch([pa.array([1, -32000, 32000], type=pa.int16())], schema=schema)

    result = _do_exchange(flight_client, call_options,
                          "SELECT v FROM _exchange_input ORDER BY v", schema, [batch])
    assert result.column("v").to_pylist() == [-32000, 1, 32000]


def test_exchange_float32(flight_client, call_options):
    """FLOAT (real) round-trip."""
    schema = pa.schema([pa.field("v", pa.float32())])
    batch = pa.record_batch([pa.array([1.5, -0.25], type=pa.float32())], schema=schema)

    result = _do_exchange(flight_client, call_options,
                          "SELECT v FROM _exchange_input ORDER BY v", schema, [batch])
    vals = result.column("v").to_pylist()
    assert abs(vals[0] - (-0.25)) < 0.001
    assert abs(vals[1] - 1.5) < 0.001


# ── String edge cases ──

def test_exchange_special_strings(flight_client, call_options):
    """Single quotes, empty strings, unicode."""
    schema = pa.schema([pa.field("s", pa.string())])
    batch = pa.record_batch([
        pa.array(["it's", "", "héllo wörld", "line1\nline2"], type=pa.string()),
    ], schema=schema)

    query = "SELECT s FROM _exchange_input ORDER BY s"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    vals = result.column("s").to_pylist()
    assert len(vals) == 4
    assert "it's" in vals
    assert "" in vals
    assert "héllo wörld" in vals


# ── Multiple batches ──

def test_exchange_multiple_batches(flight_client, call_options):
    """Send data in multiple batches, verify they all arrive."""
    schema = pa.schema([pa.field("id", pa.int32())])
    batches = [
        pa.record_batch([pa.array([1, 2, 3], type=pa.int32())], schema=schema),
        pa.record_batch([pa.array([4, 5], type=pa.int32())], schema=schema),
        pa.record_batch([pa.array([6], type=pa.int32())], schema=schema),
    ]

    query = "SELECT COUNT(*) AS cnt, SUM(id) AS total FROM _exchange_input"
    result = _do_exchange(flight_client, call_options, query, schema, batches)

    assert result.column("cnt").to_pylist() == [6]
    assert result.column("total").to_pylist() == [21]


# ── Query patterns ──

def test_exchange_subquery(flight_client, call_options):
    """Use _exchange_input inside a subquery."""
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("score", pa.float64()),
    ])
    batch = pa.record_batch([
        pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        pa.array([10.0, 90.0, 50.0, 80.0, 20.0], type=pa.float64()),
    ], schema=schema)

    query = ("SELECT id, score FROM _exchange_input "
             "WHERE score > (SELECT AVG(score) FROM _exchange_input) ORDER BY id")
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    ids = result.column("id").to_pylist()
    assert ids == [2, 4]  # avg=50, only 90 and 80 above


def test_exchange_window_function(flight_client, call_options):
    """Window function over exchange input."""
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("val", pa.float64()),
    ])
    batch = pa.record_batch([
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array([10.0, 30.0, 20.0], type=pa.float64()),
    ], schema=schema)

    query = ("SELECT id, val, RANK() OVER (ORDER BY val DESC) AS rnk "
             "FROM _exchange_input ORDER BY id")
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.column("rnk").to_pylist() == [3, 1, 2]


def test_exchange_join_with_generate_series(flight_client, call_options):
    """Join exchange input against a server-side generate_series."""
    schema = pa.schema([pa.field("n", pa.int32())])
    batch = pa.record_batch([pa.array([2, 5, 8], type=pa.int32())], schema=schema)

    query = ("SELECT e.n FROM _exchange_input e "
             "JOIN generate_series(1, 10) g(v) ON e.n = g.v "
             "ORDER BY e.n")
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.column("n").to_pylist() == [2, 5, 8]


def test_exchange_single_column_single_row(flight_client, call_options):
    """Minimal: 1 column, 1 row."""
    schema = pa.schema([pa.field("x", pa.int32())])
    batch = pa.record_batch([pa.array([42], type=pa.int32())], schema=schema)

    result = _do_exchange(flight_client, call_options,
                          "SELECT x * 2 AS doubled FROM _exchange_input", schema, [batch])
    assert result.column("doubled").to_pylist() == [84]


def test_exchange_wide_columns(flight_client, call_options):
    """10 columns of mixed types."""
    fields = [pa.field(f"c{i}", pa.int32()) for i in range(10)]
    schema = pa.schema(fields)
    arrays = [pa.array([i * 10 + j for j in range(3)], type=pa.int32()) for i in range(10)]
    batch = pa.record_batch(arrays, schema=schema)

    query = "SELECT c0, c5, c9 FROM _exchange_input ORDER BY c0"
    result = _do_exchange(flight_client, call_options, query, schema, [batch])

    assert result.num_rows == 3
    assert result.column("c0").to_pylist() == [0, 1, 2]
    assert result.column("c5").to_pylist() == [50, 51, 52]
    assert result.column("c9").to_pylist() == [90, 91, 92]
