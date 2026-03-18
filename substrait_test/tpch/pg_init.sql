DROP SCHEMA IF EXISTS tpch CASCADE;
CREATE SCHEMA tpch;

CREATE TABLE tpch.customer (
  c_custkey BIGINT,
  c_name VARCHAR(25),
  c_address VARCHAR(40),
  c_nationkey BIGINT,
  c_phone CHAR(15),
  c_acctbal DECIMAL,
  c_mktsegment CHAR(10),
  c_comment VARCHAR(117)
);

CREATE TABLE tpch.lineitem (
  l_orderkey BIGINT,
  l_partkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber INTEGER,
  l_quantity DECIMAL,
  l_extendedprice DECIMAL,
  l_discount DECIMAL,
  l_tax DECIMAL,
  l_returnflag CHAR(1),
  l_linestatus CHAR(1),
  l_shipdate DATE,
  l_commitdate DATE,
  l_receiptdate DATE,
  l_shipinstruct CHAR(25),
  l_shipmode CHAR(10),
  l_comment VARCHAR(44)
);

CREATE TABLE tpch.nation (
  n_nationkey BIGINT,
  n_name CHAR(25),
  n_regionkey BIGINT,
  n_comment VARCHAR(152)
);

CREATE TABLE tpch.orders (
  o_orderkey BIGINT,
  o_custkey BIGINT,
  o_orderstatus CHAR(1),
  o_totalprice DECIMAL,
  o_orderdate DATE,
  o_orderpriority CHAR(15),
  o_clerk CHAR(15),
  o_shippriority INTEGER,
  o_comment VARCHAR(79)
);

CREATE TABLE tpch.part (
  p_partkey BIGINT,
  p_name VARCHAR(55),
  p_mfgr CHAR(25),
  p_brand CHAR(10),
  p_type VARCHAR(25),
  p_size INTEGER,
  p_container CHAR(10),
  p_retailprice DECIMAL,
  p_comment VARCHAR(23)
);

CREATE TABLE tpch.partsupp (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INTEGER,
  ps_supplycost DECIMAL,
  ps_comment VARCHAR(199)
);

CREATE TABLE tpch.region (
  r_regionkey BIGINT,
  r_name CHAR(25),
  r_comment VARCHAR(152)
);

CREATE TABLE tpch.supplier (
  s_suppkey BIGINT,
  s_name CHAR(25),
  s_address VARCHAR(40),
  s_nationkey BIGINT,
  s_phone CHAR(15),
  s_acctbal DECIMAL,
  s_comment VARCHAR(101)
);

-- Indexes for correlated subquery performance
CREATE INDEX idx_lineitem_orderkey ON tpch.lineitem (l_orderkey);
CREATE INDEX idx_lineitem_partkey_suppkey ON tpch.lineitem (l_partkey, l_suppkey);
CREATE INDEX idx_lineitem_suppkey ON tpch.lineitem (l_suppkey);
CREATE INDEX idx_orders_orderkey ON tpch.orders (o_orderkey);
CREATE INDEX idx_partsupp_partkey ON tpch.partsupp (ps_partkey);
CREATE INDEX idx_supplier_suppkey ON tpch.supplier (s_suppkey);
CREATE INDEX idx_part_partkey ON tpch.part (p_partkey);
CREATE INDEX idx_nation_nationkey ON tpch.nation (n_nationkey);
CREATE INDEX idx_customer_custkey ON tpch.customer (c_custkey);

ALTER DATABASE postgres SET search_path TO public, tpch;
