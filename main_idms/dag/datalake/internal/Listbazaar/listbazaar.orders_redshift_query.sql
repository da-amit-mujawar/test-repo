
DROP TABLE IF EXISTS spectrumdb.listbazaar_orders;

CREATE EXTERNAL TABLE spectrumdb.listbazaar_orders (
	order_id numeric(10, 0),
	order_desc varchar(40),
	x_account_id varchar(32),
	account_type char(1),
	product_id smallint,
	status_id smallint,
	start timestamp,
	finish timestamp,
	parent_id DECIMAL(10, 0),
	page_id smallint,
	session_id char(15),
	order_count int,
	ship_method varchar(40),
	ship_no varchar(9),
	retail_fee DOUBLE PRECISION,
	discount_fee DOUBLE PRECISION,
	ship_fee DOUBLE PRECISION,
	tax_fee DOUBLE PRECISION,
	total_fee DOUBLE PRECISION,
	prod_usage varchar(20),
	po_no varchar(12),
	order_no char(10),
	ship_comp varchar(80),
	ship_att varchar(60),
	ship_addr varchar(80),
	ship_city varchar(30),
	ship_state char(2),
	ship_zip varchar(10),
	cc_type varchar(20),
	cc_no varchar(160),
	cc_exp_date varchar(6),
	cc_name varchar(60),
	cc_addr varchar(22),
	auth_code varchar(10),
	confirm_no varchar(10),
	vendor_id varchar(6),
	promotion_id numeric(10, 0),
	updates_flag char(1),
	cc_trace_no varchar(10),
	cc_auth_date timestamp,
	cc_city varchar(30),
	cc_state char(2),
	cc_zip varchar(10),
	cc_country varchar(30),
	list_name varchar(80),
	list_purpose varchar(255),
	list_term_accepted smallint,
	dist_company_code char(2),
	salerep_id varchar(6),
	account_id varchar(60),
	invoice_no varchar(20),
	s_order_id numeric(10, 0),
	s_auth_code varchar(10),
	division_id varchar(6),
	territory_id int,
	additional_email varchar(60),
	activation_invoice varchar(20),
	media_code varchar(20),
	external_id varchar(25),
	product_code varchar(8),
	suppres_search smallint,
	MigratedOrderId varchar(100),
	sg_migration_flag smallint,
	internal_order_flag smallint,
	deleted_flag smallint,
	ordered_flag smallint,
	saved_search_to_order_id numeric(10,0),
	sensitive_flag smallint,
	dnc_flag smallint,
	sensitive_approved_flag smallint,
	dnc_approved_flag smallint,
	additional_email_count int,
	sample_flag smallint,
	download_url varchar(1024),
	record_obsolescence_date timestamp,
	auto_saved_flag smallint,
	pending_order_flag smallint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/listbazaar/listbazaar_orders/'
--TABLE PROPERTIES ('skip.header.line.count'='1','compression_type'='gzip');

 
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.listbazaar_orders;

CREATE VIEW interna.listbazaar_orders
AS 
SELECT * 
  FROM spectrumdb.listbazaar_orders
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) FROM interna.listbazaar_orders
SELECT * FROM interna.listbazaar_orders

select count(*)from spectrumdb.listbazaar_orders

select top 100 * from spectrumdb.listbazaar_orders