"""FDW write (INSERT) tests using psql subprocess."""
import pytest


@pytest.fixture(autouse=True)
def clean_write_target(psql, fdw_tables):
    """Truncate write target before each test."""
    psql("TRUNCATE fdw_write_target")
    yield


class TestFdwInsertBasic:
    def test_insert_single_row(self, psql):
        psql("INSERT INTO fdw_write_remote VALUES (1, 'alice', 10.5)")
        out = psql("SELECT * FROM fdw_write_target ORDER BY id")
        assert out.strip() == "1|alice|10.5"

    def test_insert_multiple_rows(self, psql):
        psql("INSERT INTO fdw_write_remote VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)")
        out = psql("SELECT COUNT(*) FROM fdw_write_target")
        assert out.strip() == "3"

    def test_insert_nulls(self, psql):
        psql("INSERT INTO fdw_write_remote VALUES (1, NULL, NULL)")
        out = psql("SELECT id, name IS NULL, value IS NULL FROM fdw_write_target")
        assert out.strip() == "1|t|t"

    def test_insert_special_strings(self, psql):
        psql("INSERT INTO fdw_write_remote VALUES (1, 'it''s a test', 0)")
        out = psql("SELECT name FROM fdw_write_target WHERE id = 1")
        assert out.strip() == "it's a test"

    def test_insert_empty_string(self, psql):
        psql("INSERT INTO fdw_write_remote VALUES (1, '', 0)")
        out = psql("SELECT name, name = '' FROM fdw_write_target WHERE id = 1")
        assert out.strip() == "|t"


class TestFdwInsertSelect:
    def test_insert_from_local_table(self, psql):
        """INSERT INTO remote SELECT ... FROM local — the coordinator pattern."""
        psql("INSERT INTO fdw_write_remote SELECT id, name, value FROM fdw_test_local")
        out = psql("SELECT COUNT(*) FROM fdw_write_target")
        assert out.strip() == "3"
        out = psql("SELECT name FROM fdw_write_target ORDER BY id")
        names = out.strip().split("\n")
        assert names == ["alice", "bob", "charlie"]

    def test_insert_from_remote_to_remote(self, psql):
        """INSERT INTO remote SELECT ... FROM remote — cross-remote copy."""
        psql("INSERT INTO fdw_write_remote SELECT id, name, value FROM fdw_test_remote")
        out = psql("SELECT SUM(value) FROM fdw_write_target")
        assert abs(float(out.strip()) - 61.5) < 0.01

    def test_insert_with_transform(self, psql):
        """INSERT with computed values."""
        psql("INSERT INTO fdw_write_remote SELECT id, UPPER(name), value * 2 FROM fdw_test_local")
        out = psql("SELECT name, value FROM fdw_write_target ORDER BY id")
        rows = [r.split("|") for r in out.strip().split("\n")]
        assert rows[0] == ["ALICE", "21"]
        assert rows[1] == ["BOB", "40.6"]


class TestFdwInsertVerify:
    def test_roundtrip_read_back(self, psql):
        """Write via FDW, read back via FDW."""
        psql("INSERT INTO fdw_write_remote VALUES (42, 'roundtrip', 99.9)")
        out = psql("SELECT * FROM fdw_write_remote WHERE id = 42")
        parts = out.strip().split("|")
        assert parts[0] == "42"
        assert parts[1] == "roundtrip"
        assert abs(float(parts[2]) - 99.9) < 0.01

    def test_insert_many_rows(self, psql):
        """Batch INSERT of 200 rows — exercises flush logic."""
        psql("INSERT INTO fdw_write_remote "
             "SELECT g, 'row' || g, g::double precision "
             "FROM generate_series(1, 200) g")
        out = psql("SELECT COUNT(*) FROM fdw_write_target")
        assert out.strip() == "200"
        out = psql("SELECT SUM(value) FROM fdw_write_target")
        assert int(float(out.strip())) == 20100  # sum(1..200)
