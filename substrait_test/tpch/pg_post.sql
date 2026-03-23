SET maintenance_work_mem = '1GB';

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
