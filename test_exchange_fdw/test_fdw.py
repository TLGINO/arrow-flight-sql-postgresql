"""FDW tests using psql subprocess."""
import pytest


class TestFdwSelect:
    def test_select_all(self, psql, fdw_tables):
        out = psql("SELECT * FROM fdw_test_remote ORDER BY id")
        rows = [r.split("|") for r in out.strip().split("\n")]
        assert len(rows) == 3
        assert rows[0] == ["1", "alice", "10.5"]
        assert rows[1] == ["2", "bob", "20.3"]
        assert rows[2] == ["3", "charlie", "30.7"]

    def test_select_columns(self, psql, fdw_tables):
        out = psql("SELECT name FROM fdw_test_remote WHERE id = 2")
        assert out.strip() == "bob"


class TestFdwJoin:
    def test_join_local_remote(self, psql, fdw_tables):
        sql = """
        SELECT l.id, l.name, r.value
        FROM fdw_test_local l
        JOIN fdw_test_remote r ON l.id = r.id
        WHERE r.value > 15
        ORDER BY l.id
        """
        out = psql(sql)
        rows = [r.split("|") for r in out.strip().split("\n")]
        assert len(rows) == 2
        assert rows[0][1] == "bob"
        assert rows[1][1] == "charlie"


class TestFdwExplain:
    def test_explain_shows_remote_query(self, psql, fdw_tables):
        out = psql("EXPLAIN SELECT * FROM fdw_test_remote")
        assert "Foreign Scan" in out
        assert "Remote Query" in out
        assert "fdw_test_local" in out  # table_name option

    def test_explain_shows_server(self, psql, fdw_tables):
        out = psql("EXPLAIN SELECT * FROM fdw_test_remote")
        assert "Flight SQL Server" in out
        assert "grpc://" in out


class TestFdwWhere:
    def test_where_filter(self, psql, fdw_tables):
        out = psql("SELECT name FROM fdw_test_remote WHERE value > 25 ORDER BY name")
        assert out.strip() == "charlie"

    def test_where_no_results(self, psql, fdw_tables):
        out = psql("SELECT COUNT(*) FROM fdw_test_remote WHERE value > 100")
        assert out.strip() == "0"


class TestFdwMultipleScans:
    def test_sequential_selects(self, psql, fdw_tables):
        out1 = psql("SELECT COUNT(*) FROM fdw_test_remote")
        assert out1.strip() == "3"
        out2 = psql("SELECT COUNT(*) FROM fdw_test_remote")
        assert out2.strip() == "3"

    def test_two_foreign_tables(self, psql, fdw_tables):
        out = psql("SELECT total_value FROM fdw_test_query")
        total = float(out.strip())
        assert abs(total - 61.5) < 0.01  # 10.5 + 20.3 + 30.7


class TestFdwCustomQuery:
    def test_query_option(self, psql, fdw_tables):
        out = psql("SELECT * FROM fdw_test_query")
        total = float(out.strip())
        assert abs(total - 61.5) < 0.01

    def test_query_with_where(self, psql, fdw_tables):
        """Custom query foreign table still works with local WHERE (post-filter)."""
        out = psql("SELECT total_value FROM fdw_test_query WHERE total_value > 50")
        total = float(out.strip())
        assert abs(total - 61.5) < 0.01

    def test_query_no_match(self, psql, fdw_tables):
        """Custom query result filtered out by local WHERE."""
        out = psql("SELECT COUNT(*) FROM fdw_test_query WHERE total_value > 999")
        assert out.strip() == "0"


class TestFdwAggregation:
    def test_count(self, psql, fdw_tables):
        out = psql("SELECT COUNT(*) FROM fdw_test_remote")
        assert out.strip() == "3"

    def test_sum(self, psql, fdw_tables):
        out = psql("SELECT SUM(value) FROM fdw_test_remote")
        assert abs(float(out.strip()) - 61.5) < 0.01

    def test_avg(self, psql, fdw_tables):
        out = psql("SELECT ROUND(AVG(value)::numeric, 2) FROM fdw_test_remote")
        assert out.strip() == "20.50"

    def test_min_max(self, psql, fdw_tables):
        out = psql("SELECT MIN(value), MAX(value) FROM fdw_test_remote")
        parts = out.strip().split("|")
        assert abs(float(parts[0]) - 10.5) < 0.01
        assert abs(float(parts[1]) - 30.7) < 0.01


class TestFdwOrderAndLimit:
    def test_order_asc(self, psql, fdw_tables):
        out = psql("SELECT name FROM fdw_test_remote ORDER BY value ASC")
        names = out.strip().split("\n")
        assert names == ["alice", "bob", "charlie"]

    def test_order_desc(self, psql, fdw_tables):
        out = psql("SELECT name FROM fdw_test_remote ORDER BY value DESC")
        names = out.strip().split("\n")
        assert names == ["charlie", "bob", "alice"]

    def test_limit(self, psql, fdw_tables):
        out = psql("SELECT name FROM fdw_test_remote ORDER BY id LIMIT 2")
        names = out.strip().split("\n")
        assert names == ["alice", "bob"]

    def test_limit_offset(self, psql, fdw_tables):
        out = psql("SELECT name FROM fdw_test_remote ORDER BY id LIMIT 1 OFFSET 1")
        assert out.strip() == "bob"


class TestFdwSubqueries:
    def test_in_subquery(self, psql, fdw_tables):
        sql = ("SELECT name FROM fdw_test_remote "
               "WHERE id IN (SELECT id FROM fdw_test_local WHERE value > 15) ORDER BY id")
        out = psql(sql)
        names = out.strip().split("\n")
        assert names == ["bob", "charlie"]

    def test_exists(self, psql, fdw_tables):
        sql = ("SELECT name FROM fdw_test_remote r "
               "WHERE EXISTS (SELECT 1 FROM fdw_test_local l "
               "WHERE l.id = r.id AND l.value > 25) ORDER BY id")
        out = psql(sql)
        assert out.strip() == "charlie"

    def test_scalar_subquery(self, psql, fdw_tables):
        sql = ("SELECT name FROM fdw_test_remote "
               "WHERE value > (SELECT AVG(value) FROM fdw_test_remote) ORDER BY id")
        out = psql(sql)
        assert out.strip() == "charlie"


class TestFdwCTE:
    def test_cte_with_foreign_table(self, psql, fdw_tables):
        sql = """
        WITH top AS (
          SELECT * FROM fdw_test_remote WHERE value > 15 ORDER BY value DESC
        )
        SELECT name, value FROM top ORDER BY value
        """
        out = psql(sql)
        rows = [r.split("|") for r in out.strip().split("\n")]
        assert len(rows) == 2
        assert rows[0][0] == "bob"
        assert rows[1][0] == "charlie"


class TestFdwSelfJoin:
    def test_self_join(self, psql, fdw_tables):
        """Join the foreign table against itself."""
        sql = """
        SELECT a.name, b.name
        FROM fdw_test_remote a
        JOIN fdw_test_remote b ON a.id = b.id - 1
        ORDER BY a.id
        """
        out = psql(sql)
        rows = [r.split("|") for r in out.strip().split("\n")]
        assert len(rows) == 2
        assert rows[0] == ["alice", "bob"]
        assert rows[1] == ["bob", "charlie"]


class TestFdwDistinct:
    def test_distinct_values(self, psql, fdw_tables):
        """DISTINCT on foreign table column."""
        out = psql("SELECT DISTINCT name FROM fdw_test_remote ORDER BY name")
        names = out.strip().split("\n")
        assert names == ["alice", "bob", "charlie"]
