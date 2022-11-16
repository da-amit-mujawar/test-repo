
DROP TABLE IF EXISTS spectrumdb.listbazaar_accounts;

CREATE EXTERNAL TABLE spectrumdb.listbazaar_accounts (
   account_id              varchar(60),
   x_account_id            varchar(32),
   account_desc            varchar(40),
   password                varchar(40),
   m_maiden_name           varchar(40),
   last_name               varchar(40),
   first_name              varchar(20),
   company                 varchar(80),
   address                 varchar(80),
   city                    varchar(30),
   state                   varchar(30),
   zip                     varchar(10),
   country                 varchar(30),
   phone_no                varchar(16),
   fax_no                  varchar(16),
   email                   varchar(60),
   created_date            timestamp  ,
   init_registrar          char(4)    ,
   status_id               smallint   ,
   notes                   varchar(255),
   company_id              smallint   ,
   media_id                varchar(20),
   segment_id              smallint   ,
   salerep_id              varchar(6) ,
   abi_no                  char(9)    ,
   url                     varchar(80),
   advertiser_id           smallint   ,
   passwd_hint_question    varchar(255),
   passwd_hint_answer      varchar(255),
   territory_id            int        ,
   email_status            char(8)    ,
   term_accepted           smallint   ,
   vendor_id               varchar(6) ,
   service_id              smallint   ,
   customer_ref            varchar(240),
   sec_phone_no            varchar(16),
   match_flag              char(1)    ,
   archive_flag            char(1)    ,
   customer_id             decimal(8,0),
   sf_leadcontact_id       char(25)   ,
   phone_ext               char(5)    ,
   last_modified_date      timestamp  ,
   last_sf_sync_date       timestamp  ,
   industry                varchar(60),
   no_employees            varchar(60),
   think_login_id          int        ,
   think_account_id        int        ,
   think_contact_id        int        ,
   newsgaccountid          int        ,
   newsguserid             int        ,
   interested_prospects    varchar(1024),
   job_title               varchar(240),
   attach_order_file_flag  smallint   ,
   bel_approved_flag       smallint   ,
   sales_division_id       varchar(20),
   email_optout_flag       smallint   ,
   express_checkout_flag   smallint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/listbazaar/listbazaar_accounts/'
TABLE PROPERTIES ('compression_type'='gzip');

 
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.listbazaar_accounts;

CREATE VIEW interna.listbazaar_accounts 
AS 
SELECT * 
  FROM spectrumdb.listbazaar_accounts
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.listbazaar_accounts

SELECT TOP 100 * 
  FROM interna.listbazaar_accounts
