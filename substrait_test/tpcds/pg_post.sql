SET maintenance_work_mem = '1GB';

-- Indexes on commonly-joined surrogate key columns
CREATE INDEX idx_ss_sold_date ON tpcds.store_sales (ss_sold_date_sk);
CREATE INDEX idx_ss_item ON tpcds.store_sales (ss_item_sk);
CREATE INDEX idx_ss_customer ON tpcds.store_sales (ss_customer_sk);
CREATE INDEX idx_ss_store ON tpcds.store_sales (ss_store_sk);
CREATE INDEX idx_ss_cdemo ON tpcds.store_sales (ss_cdemo_sk);
CREATE INDEX idx_ss_hdemo ON tpcds.store_sales (ss_hdemo_sk);
CREATE INDEX idx_ss_addr ON tpcds.store_sales (ss_addr_sk);
CREATE INDEX idx_ss_promo ON tpcds.store_sales (ss_promo_sk);

CREATE INDEX idx_cs_sold_date ON tpcds.catalog_sales (cs_sold_date_sk);
CREATE INDEX idx_cs_item ON tpcds.catalog_sales (cs_item_sk);
CREATE INDEX idx_cs_bill_customer ON tpcds.catalog_sales (cs_bill_customer_sk);
CREATE INDEX idx_cs_ship_date ON tpcds.catalog_sales (cs_ship_date_sk);

CREATE INDEX idx_ws_sold_date ON tpcds.web_sales (ws_sold_date_sk);
CREATE INDEX idx_ws_item ON tpcds.web_sales (ws_item_sk);
CREATE INDEX idx_ws_bill_customer ON tpcds.web_sales (ws_bill_customer_sk);
CREATE INDEX idx_ws_ship_date ON tpcds.web_sales (ws_ship_date_sk);

CREATE INDEX idx_sr_item ON tpcds.store_returns (sr_item_sk);
CREATE INDEX idx_sr_customer ON tpcds.store_returns (sr_customer_sk);
CREATE INDEX idx_sr_returned_date ON tpcds.store_returns (sr_returned_date_sk);

CREATE INDEX idx_cr_item ON tpcds.catalog_returns (cr_item_sk);
CREATE INDEX idx_cr_returned_date ON tpcds.catalog_returns (cr_returned_date_sk);

CREATE INDEX idx_wr_item ON tpcds.web_returns (wr_item_sk);
CREATE INDEX idx_wr_returned_date ON tpcds.web_returns (wr_returned_date_sk);

CREATE INDEX idx_customer_addr ON tpcds.customer (c_current_addr_sk);
CREATE INDEX idx_customer_cdemo ON tpcds.customer (c_current_cdemo_sk);
CREATE INDEX idx_customer_hdemo ON tpcds.customer (c_current_hdemo_sk);

CREATE INDEX idx_inv_item ON tpcds.inventory (inv_item_sk);
CREATE INDEX idx_inv_date ON tpcds.inventory (inv_date_sk);
CREATE INDEX idx_inv_warehouse ON tpcds.inventory (inv_warehouse_sk);

-- Dimension filter columns (high use across queries)
CREATE INDEX idx_dd_year ON tpcds.date_dim (d_year);
CREATE INDEX idx_dd_moy ON tpcds.date_dim (d_moy);
CREATE INDEX idx_customer_id ON tpcds.customer (c_customer_id);
CREATE INDEX idx_item_category ON tpcds.item (i_category);
CREATE INDEX idx_cd_marital ON tpcds.customer_demographics (cd_marital_status);
CREATE INDEX idx_cd_education ON tpcds.customer_demographics (cd_education_status);
CREATE INDEX idx_hd_dep_count ON tpcds.household_demographics (hd_dep_count);
CREATE INDEX idx_ca_state ON tpcds.customer_address (ca_state);
CREATE INDEX idx_ca_country ON tpcds.customer_address (ca_country);
CREATE INDEX idx_ca_gmt_offset ON tpcds.customer_address (ca_gmt_offset);

-- Fact table FK columns (medium priority)
CREATE INDEX idx_ss_sold_time ON tpcds.store_sales (ss_sold_time_sk);
CREATE INDEX idx_cs_warehouse ON tpcds.catalog_sales (cs_warehouse_sk);
CREATE INDEX idx_cs_call_center ON tpcds.catalog_sales (cs_call_center_sk);
CREATE INDEX idx_ws_warehouse ON tpcds.web_sales (ws_warehouse_sk);
CREATE INDEX idx_ws_web_site ON tpcds.web_sales (ws_web_site_sk);
CREATE INDEX idx_ws_web_page ON tpcds.web_sales (ws_web_page_sk);
CREATE INDEX idx_ws_bill_addr ON tpcds.web_sales (ws_bill_addr_sk);

-- Return table columns
CREATE INDEX idx_cr_ret_customer ON tpcds.catalog_returns (cr_returning_customer_sk);
CREATE INDEX idx_wr_ret_customer ON tpcds.web_returns (wr_returning_customer_sk);
CREATE INDEX idx_cr_call_center ON tpcds.catalog_returns (cr_call_center_sk);

-- Item dimension filter columns
CREATE INDEX idx_item_brand_id ON tpcds.item (i_brand_id);
CREATE INDEX idx_item_class_id ON tpcds.item (i_class_id);
CREATE INDEX idx_item_category_id ON tpcds.item (i_category_id);
CREATE INDEX idx_item_manufact_id ON tpcds.item (i_manufact_id);

ALTER DATABASE postgres SET search_path TO public, tpcds;
