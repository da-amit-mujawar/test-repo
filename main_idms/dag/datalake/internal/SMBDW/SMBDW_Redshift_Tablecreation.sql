
DROP TABLE IF EXISTS spectrumdb.SMBDWstaging_bart_accounts; 
CREATE EXTERNAL TABLE spectrumdb.SMBDWstaging_bart_accounts 
(
id INT
,AccountId INT
,SalesForceId varchar(20)
,ThinkId INT
,AccountNumber CHAR(10)
,Name VARCHAR(80)
,Address1 VARCHAR(50)
,Address2 VARCHAR(50)
,City VARCHAR(50)
,State VARCHAR(20)
,ZipCode VARCHAR(20)
,Country VARCHAR(50)
,Phone VARCHAR(25)
,Email VARCHAR(256)
,MediaCode VARCHAR(50)
,StartDate TIMESTAMP
,EndDate TIMESTAMP
,CreateDate TIMESTAMP
,AccountType SMALLINT
,SalesRepId INT
,IsActive int
,IsEnterprise int
,FileManagementId varchar(64)
,OracleSalesRepId VARCHAR(20)
,DatalynxId INT
,WhiteLabelId INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDWstaging_bart_accounts/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDWstaging_bart_accounts; 
CREATE VIEW interna.SMBDWstaging_bart_accounts AS SELECT * FROM spectrumdb.SMBDWstaging_bart_accounts WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDWstaging_bart_accounts; 
SELECT TOP 100 * FROM interna.SMBDWstaging_bart_accounts; 


DROP TABLE IF EXISTS spectrumdb.SMBDWstaging_bart_users; 
CREATE EXTERNAL TABLE spectrumdb.SMBDWstaging_bart_users 
(
id INT
,UserId INT
,AccountId INT
,SalesForceId varchar(20)
,ThinkId INT
,UserName varchar(256)
,FirstName varchar(128)
,LastName varchar(128)
,UserEmail varchar(256)
,IsEnterprise int
,StartDate TIMESTAMP
,EndDate TIMESTAMP
,CreatedDate TIMESTAMP
,SubscriptionType SMALLINT
,AuthId varchar(64)
,IsActive int
,FileManagementId varchar(64)
,OracleSalesRepId VARCHAR(20)
,DatalynxId INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDWstaging_bart_users/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDWstaging_bart_users; 
CREATE VIEW interna.SMBDWstaging_bart_users AS SELECT * FROM spectrumdb.SMBDWstaging_bart_users WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDWstaging_bart_users; 
SELECT TOP 100 * FROM interna.SMBDWstaging_bart_users; 


DROP TABLE IF EXISTS spectrumdb.SMBDWstaging_bart_EmailCreditTransactions_all; 
CREATE EXTERNAL TABLE spectrumdb.SMBDWstaging_bart_EmailCreditTransactions_all 
(
UserId INT
,CreditAdjustment INT
,"Time" TIMESTAMP
,IsAdjustment int
,PurchaseAmount DECIMAL(18,2)
,TransactionId VARCHAR(64)
,VendorKey VARCHAR(6)
,TransactionType SMALLINT
,PricePerCredit DECIMAL(18,4)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDWstaging_bart_EmailCreditTransactions_all/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDWstaging_bart_EmailCreditTransactions_all; 
CREATE VIEW interna.SMBDWstaging_bart_EmailCreditTransactions_all AS SELECT * FROM spectrumdb.SMBDWstaging_bart_EmailCreditTransactions_all WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDWstaging_bart_EmailCreditTransactions_all; 
SELECT TOP 100 * FROM interna.SMBDWstaging_bart_EmailCreditTransactions_all; 


DROP TABLE IF EXISTS spectrumdb.SMBDWstaging_bart_CustomerDataCreditTransactions; 
CREATE EXTERNAL TABLE spectrumdb.SMBDWstaging_bart_CustomerDataCreditTransactions 
(
Id INT
,UserId INT
,LimitAdjustment INT
,"Time" TIMESTAMP
,TransactionType SMALLINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDWstaging_bart_CustomerDataCreditTransactions/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDWstaging_bart_CustomerDataCreditTransactions; 
CREATE VIEW interna.SMBDWstaging_bart_CustomerDataCreditTransactions AS SELECT * FROM spectrumdb.SMBDWstaging_bart_CustomerDataCreditTransactions WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDWstaging_bart_CustomerDataCreditTransactions; 
SELECT TOP 100 * FROM interna.SMBDWstaging_bart_CustomerDataCreditTransactions; 



DROP TABLE IF EXISTS spectrumdb.SMBDWstaging_bart_credit_transactions; 
CREATE EXTERNAL TABLE spectrumdb.SMBDWstaging_bart_credit_transactions 
(
id INT
,UserId INT
,TransactionType varchar(50)
,CreditAdjustment INT
,"Time" TIMESTAMP
,ThinkOrderId INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDWstaging_bart_credit_transactions/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDWstaging_bart_credit_transactions; 
CREATE VIEW interna.SMBDWstaging_bart_credit_transactions AS SELECT * FROM spectrumdb.SMBDWstaging_bart_credit_transactions WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDWstaging_bart_credit_transactions; 
SELECT TOP 100 * FROM interna.SMBDWstaging_bart_credit_transactions;

DROP TABLE IF EXISTS spectrumdb.SMBDWstaging_auth_users; 
CREATE EXTERNAL TABLE spectrumdb.SMBDWstaging_auth_users 
(
id INT
,UserId Varchar(64)
,CreateDate TIMESTAMP
,LastLoginDate TIMESTAMP
,SalesforceAccountId varchar(20)
,SalesforceUserId varchar(20)
,SiteName VARCHAR(128)
,RequiresTwoFactorAuth int
,LastLoginSource varchar(64)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDWstaging_auth_users/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDWstaging_auth_users; 
CREATE VIEW interna.SMBDWstaging_auth_users AS SELECT * FROM spectrumdb.SMBDWstaging_auth_users WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDWstaging_auth_users; 
SELECT TOP 100 * FROM interna.SMBDWstaging_auth_users; 

DROP TABLE IF EXISTS spectrumdb.SMBDWstaging_bart_credit_used; 
CREATE EXTERNAL TABLE spectrumdb.SMBDWstaging_bart_credit_used 
(
UserId INT
,Credits INT
,CreditsUsed INT
,IsActive INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDWstaging_bart_credit_used/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDWstaging_bart_credit_used; 
CREATE VIEW interna.SMBDWstaging_bart_credit_used AS SELECT * FROM spectrumdb.SMBDWstaging_bart_credit_used WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDWstaging_bart_credit_used; 
SELECT TOP 100 * FROM interna.SMBDWstaging_bart_credit_used; 

DROP TABLE IF EXISTS spectrumdb.SMBDW_dim_oess_order_params; 
CREATE EXTERNAL TABLE spectrumdb.SMBDW_dim_oess_order_params 
(
order_params_id INT
,order_what varchar(50)
,order_type varchar(50)
,origin_app varchar(50)
,order_status varchar(50)
,oess_order_status varchar(50)
,payment_frequency varchar(50)
,oess_order_flow varchar(50)
,vertical varchar(50)
,site varchar(50)
,fulfillment varchar(50)
,contract_obligation varchar(50)
,contract_update_frequency varchar(50)
,device_type varchar(16)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDW_dim_oess_order_params/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDW_dim_oess_order_params; 
CREATE VIEW interna.SMBDW_dim_oess_order_params AS SELECT * FROM spectrumdb.SMBDW_dim_oess_order_params WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDW_dim_oess_order_params; 
SELECT TOP 100 * FROM interna.SMBDW_dim_oess_order_params; 

DROP TABLE IF EXISTS spectrumdb.SMBDW_dim_sales_rep_division; 
CREATE EXTERNAL TABLE spectrumdb.SMBDW_dim_sales_rep_division 
(
sales_rep_id INT
,sales_rep_number INT
,sales_rep_name varchar(128)
,division_id varchar(16)
,division_name varchar(128)
,active int
,"current" int
,business_unit varchar(50)
,network_login varchar(50)
,email varchar(128)
,phone varchar(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDW_dim_sales_rep_division/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDW_dim_sales_rep_division; 
CREATE VIEW interna.SMBDW_dim_sales_rep_division AS SELECT * FROM spectrumdb.SMBDW_dim_sales_rep_division WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDW_dim_sales_rep_division; 
SELECT TOP 100 * FROM interna.SMBDW_dim_sales_rep_division; 


DROP TABLE IF EXISTS spectrumdb.SMBDW_dim_date; 
CREATE EXTERNAL TABLE spectrumdb.SMBDW_dim_date 
(
date_id INT
,short_date DATE
,Day_Of_Week varchar(30)
,WeekNumberOfYear INT
,Calendar_Month varchar(30)
,Calendar_Year INT
,Calendar_Year_Month varchar(50)
,Calendar_Quarter varchar(50)
,Calendar_Year_Quarter varchar(50)
,Calendar_Half_Year varchar(50)
,Corporate_Holiday CHAR(1)
,Day_of_Month INT
,Day_of_Year INT
,Day_Type varchar(50)
,Business_Day_Number INT
,Business_Day_Flag int
,FullDate TIMESTAMP
,year_month_num INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDW_dim_date/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDW_dim_date; 
CREATE VIEW interna.SMBDW_dim_date AS SELECT * FROM spectrumdb.SMBDW_dim_date WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDW_dim_date; 
SELECT TOP 100 * FROM interna.SMBDW_dim_date; 

DROP TABLE IF EXISTS spectrumdb.SMBDW_dim_oess_customers; 
CREATE EXTERNAL TABLE spectrumdb.SMBDW_dim_oess_customers 
(
oess_customer_number INT
,oracle_customer_number INT
,contact_name varchar(80)
,contact_email varchar(80)
,contact_phone varchar(15)
,account_phone varchar(15)
,account_name varchar(80)
,address varchar(80)
,city varchar(80)
,state varchar(50)
,zip varchar(15)
,is_internal int
,billing_address varchar(80)
,billing_city varchar(80)
,billing_state varchar(50)
,billing_zip varchar(15)
,oracle_customer_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDW_dim_oess_customers/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDW_dim_oess_customers; 
CREATE VIEW interna.SMBDW_dim_oess_customers AS SELECT * FROM spectrumdb.SMBDW_dim_oess_customers WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDW_dim_oess_customers; 
SELECT TOP 100 * FROM interna.SMBDW_dim_oess_customers; 

DROP TABLE IF EXISTS spectrumdb.SMBDW_fact_oess_orders; 
CREATE EXTERNAL TABLE spectrumdb.SMBDW_fact_oess_orders 
(
oess_order_id INT
,original_order_id INT
,original_subscription_id INT
,order_params_id INT
,oess_customer_number INT
,sales_rep_id INT
,media_code_id INT
,retail_price decimal(15,4)
,final_price decimal(15,4)
,discount decimal(15,4)
,created_date_id INT
,start_date_id INT
,end_date_id INT
,cancel_date_id INT
,licenses INT
,credits INT
,evergreen int
,length_months SMALLINT
,months_left SMALLINT
,monthly_value decimal(15,4)
,demographics_id INT
,sales_rep_1_pct NUMERIC
,sales_rep_2_id INT
,sales_rep_2_pct NUMERIC
,sales_rep_3_id INT
,sales_rep_3_pct NUMERIC
,is_renewal int
,is_amendment int
,contract_start_date_id INT
,contract_end_date_id INT
,IsCurrent int
,WhiteLabelText VARCHAR(255)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDW_fact_oess_orders/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDW_fact_oess_orders; 
CREATE VIEW interna.SMBDW_fact_oess_orders AS SELECT * FROM spectrumdb.SMBDW_fact_oess_orders WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDW_fact_oess_orders; 
SELECT TOP 100 * FROM interna.SMBDW_fact_oess_orders;

DROP TABLE IF EXISTS spectrumdb.SMBDW_fact_oess_cancelations; 
CREATE EXTERNAL TABLE spectrumdb.SMBDW_fact_oess_cancelations 
(
oess_order_id INT
,oess_customer_number INT
,original_subscription_id INT
,place_in_chain SMALLINT
,order_params_id INT
,cancel_reason_id INT
,cancel_note_id INT
,sales_rep_id INT
,drop_dead_date_id INT
,cancel_date_id INT
,sub_start_date_id INT
,sub_end_date_id INT
,original_sub_start_date_id INT
,monthly_value decimal(15,4)
,length_months SMALLINT
,unbilled_months SMALLINT
,WhiteLabelText VARCHAR(255)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/smbdwstaging/SMBDW_fact_oess_cancelations/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.SMBDW_fact_oess_cancelations; 
CREATE VIEW interna.SMBDW_fact_oess_cancelations AS SELECT * FROM spectrumdb.SMBDW_fact_oess_cancelations WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.SMBDW_fact_oess_cancelations; 
SELECT TOP 100 * FROM interna.SMBDW_fact_oess_cancelations; 
