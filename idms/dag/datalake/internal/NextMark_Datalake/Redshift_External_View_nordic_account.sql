DROP TABLE IF EXISTS spectrumdb.dmm_report_nordic_account; 
CREATE EXTERNAL TABLE spectrumdb.dmm_report_nordic_account 
(
ID INT
,Account_Description VARCHAR(100)
,Account_Name VARCHAR(100)
,Account_Number VARCHAR(50)
,Account_Rep_Full_Name VARCHAR(100)
,Account_Roles VARCHAR(47)
,Account_Status VARCHAR(10)
,Account_Type VARCHAR(100)
,AP_Credit_Limit NUMERIC
,AP_Terms VARCHAR(10)
,AR_Balance_Due_Amt NUMERIC
,AR_Credit_Limit NUMERIC
,AR_Terms VARCHAR(100)
,Bill_To_Address VARCHAR(100)
,Bill_To_Full_Address VARCHAR(255)
,Bill_To_City VARCHAR(80)
,Bill_To_Country VARCHAR(20)
,Bill_To_Postal_Code VARCHAR(10)
,Bill_To_State CHAR(2)
,Billing_Contact_Full_Name VARCHAR(100)
,Broker_Flag CHAR(1)
,Date_Created TIMESTAMP
,Date_Last_Ordered TIMESTAMP
,Date_Last_Updated TIMESTAMP
,Exchange_Broker_Run_Chg_Rate NUMERIC
,Exchange_Terms VARCHAR(79)
,Grace_Days INT
,Invoice_Method VARCHAR(10)
,List_Owner_Flag CHAR(1)
,Mailer_Flag CHAR(1)
,Main_Contact_Full_Name VARCHAR(100)
,Main_Fax_Number VARCHAR(24)
,Main_Phone_Number VARCHAR(24)
,Manager_Flag CHAR(1)
,Name_On_Check VARCHAR(100)
,Number_of_Contacts INT
,Number_of_Open_Invoices INT
,Parent_Account_Name VARCHAR(100)
,Segment_Standard_Discount_Pct NUMERIC
,Service_Bureau_Flag CHAR(1)
,Ship_To_Address VARCHAR(255)
,Ship_To_City VARCHAR(80)
,Ship_To_Country VARCHAR(20)
,Ship_To_Postal_Code CHAR(10)
,Ship_To_State CHAR(2)
,Standard_Discount CHAR(22)
,Tax_Ref_Number CHAR(15)
,Nm_Account_ID CHAR(22)
,Nm_Parent_Account_ID CHAR(22)
,Share_Usage CHAR(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/nextmark/account/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.dmm_report_nordic_account; 
CREATE VIEW interna.dmm_report_nordic_account AS SELECT * FROM spectrumdb.dmm_report_nordic_account WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.dmm_report_nordic_account; 
SELECT TOP 100 * FROM interna.dmm_report_nordic_account; 
