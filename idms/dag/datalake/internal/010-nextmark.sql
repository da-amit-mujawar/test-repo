DROP TABLE IF EXISTS spectrumdb.nextmark_list_Order CASCADE;

CREATE EXTERNAL TABLE spectrumdb.nextmark_list_Order 
(
  Base_Actual_Rate_Units varchar(38),
  Base_AP_Rate NUMERIC, 
  Base_AR_Rate NUMERIC,
  Base_Broker_Est_Comm_Rate NUMERIC,
  Base_Commission_Rate NUMERIC, 
  Base_Commission_Units varchar(38),
  Base_Discount_Rate NUMERIC,
  Base_Discount_Units varchar(3),
  Base_Manager_Est_Comm_Rate NUMERIC,
  Base_Name varchar(11000),
  Base_Select_Name varchar(11000),
  Base_Select_Names_w_per_Rate varchar(11000),
  Campaign_Name varchar(11000),
  Cancellation_Notes varchar(11000),
  Cancellation_Status varchar(11000),
  Clearance_Ref_number varchar(11000),
  Clearance_Status varchar(11000),
  Cleared_By varchar(11000),
  Client_Account_Name varchar(11000),
  Client_Contact_Name varchar(11000),
  Client_Credit_Approval_Notes varchar(11000),
  Client_Credit_Approved_By varchar(11000),
  Client_Credit_Required varchar(11000),
  Client_Credit_Status varchar(11000),
  Client_PO_Number varchar(11000),
  Contact_Name varchar(11000),
  Date_Cancelled DATE,
  Date_Cleared DATE,
  Date_Client_Credit_Approved DATE,
  Date_Created1 DATE,
  Date_Data_Receipt_Cutoff DATE,
  Date_Data_Received DATE,
  Date_First_Usage DATE,
  Date_Last_Updated DATE,
  Date_Last_Usage DATE,
  Date_LO_Object_Last_Updated DATE,
  Date_Mail_End DATE,
  Date_Mail_Start DATE,
  Date_Needed_By DATE,
  Date_Ordered DATE,
  Date_Revised DATE,
  Date_Shipped DATE,
  Exch_Broker_Run_Chg_Max_Amt NUMERIC,
  Exch_Broker_Run_Chg_Min_Amt NUMERIC,
  Exch_Manage_Run_Chg_Max_Amt NUMERIC,
  Exch_Manage_Run_Chg_Min_Amt NUMERIC,
  Exchange_Broker_Run_Charge_Rate NUMERIC, 
  Exchange_Broker_Run_Charge_Units varchar(3),
  Exchange_Manager_Run_Charge_Rate NUMERIC,
  Exchange_Manager_Run_Charge_Units varchar(3),
  For_Review_Flag varchar(3),
  Key_Code varchar(11000),
  Media_Name varchar(11000),
  Order_percent_Paid varchar(30),
  Order_percent_Received varchar(30),
  Order_Financial_Status varchar(11000),
  Order_Name varchar(11000),
  Order_Number varchar(11000), --# present in the number
  Order_Rent_Exch varchar(11000),
  Order_Status varchar(11000),
  Order_Test_Cont varchar(11000),
  Order_Type varchar(11000),
  Mailer_Account_Name varchar(11000),
  Mailer_SB_Account_Name varchar(11000),
  Manager_Account_Name varchar(11000),
  Mgr_Contact_Profit_Center_Code varchar(11000),
  Mgr_Contact_Profit_Center_Name varchar(11000),
  Net_Name_AP_percent numeric,
  Net_Name_AP_Run_Charge_Rate NUMERIC,
  Net_Name_AR_percent NUMERIC,
  Net_Name_AR_Run_Charge_Rate NUMERIC,
  Net_Name_Run_Charge_Units varchar(3),
  Net_Name_Run_Chg_Comm_Rate NUMERIC,
  Net_Name_Run_Chg_Comm_Units varchar(3),
  Net_Name_Run_Chg_Disc_Rate NUMERIC,
  Net_Name_Run_Chg_Disc_Units varchar(11000),
  Number_Clickthroughs NUMERIC ,
  Number_Delivered NUMERIC ,
  Number_Opens NUMERIC ,
  Number_Orders NUMERIC ,
  Number_Responses NUMERIC ,
  Number_Who_Clicked NUMERIC ,
  Offer_Description varchar(11000),
  Offer_Type varchar(11000),
  Omit_Names varchar(11000),
  Output_AP_Rate_per_F NUMERIC,
  Output_AP_Rate_per_M NUMERIC,
  Output_AR_Rate_per_F NUMERIC,
  Output_AR_Rate_per_M NUMERIC,
  Output_Commission_Rate_per_F NUMERIC, 
  Output_Commission_Rate_per_M NUMERIC,
  Output_Discount_Rate_per_F NUMERIC,
  Output_Discount_Rate_per_M NUMERIC,
  Owner_Account_Name varchar(11000),
  Owner_Exch_Ship_Bal varchar(11000),
  Owner_Parent_Account varchar(11000),
  Owner_SB_Account_Name varchar(11000),
  Preclearance_Y_n varchar(3),
  Profit_Center_Code varchar(11000),
  Profit_Center_Division varchar(11000),
  Profit_Center_Name varchar(11000),
  Qty_at_Full_Rate NUMERIC,
  Qty_at_Run_Rate NUMERIC,
  Qty_Available NUMERIC,
  Qty_Order_Exchange NUMERIC,
  Qty_Order_Rental NUMERIC,
  Qty_Order_Total NUMERIC,
  Qty_Rental_Useable_Names NUMERIC,
  Qty_Shipi_Exchange NUMERIC, 
  Qty_Ship_Rental NUMERIC,
  Qty_Ship_Total NUMERIC,
  Qty_Total_Useable_Names NUMERIC,
  Qty_Usage_to_Date NUMERIC,
  Quantity_Rule varchar(11000),
  Reuse_Type varchar(11000),
  Salesperson_Alias varchar(11000),
  Salesperson_Full_Name varchar(11000),
  Select_AP_Rate_per_F NUMERIC,
  Select_AP_Rate_per_M NUMERIC,
  Select_AR_Rate_M  NUMERIC,
  Select_AR_Rate_per_F NUMERIC,
  Select_Commission_Rate_per_F NUMERIC,
  Select_Commission_Rate_per_M NUMERIC,
  Select_Discount_Rate_per_F NUMERIC,
  Select_Discount_Rate_per_M NUMERIC,
  Select_Names varchar(11000),
  Ship_Label varchar(11000),
  Ship_To_Contact_Name varchar(11000),
  Shipping_Amount NUMERIC,
  Shipping_Method varchar(11000),
  Tax_Amount NUMERIC,
  Vendor_Credit_Status varchar(11000),
  Nm_Campaign_Id NUMERIC ,
  Nm_Client_Account_Id NUMERIC ,
  Nm_Media_Id NUMERIC ,
  Nm_Order_Id NUMERIC ,
  Nm_Mailer_Account_Id NUMERIC ,
  Nm_Mailer_Serv_Bur_Acct_Id NUMERIC ,
  Nm_Manager_Account_Id NUMERIC ,
  Nm_Owner_Acct_Id NUMERIC ,
  Nm_Owner_Sb_Account_Id NUMERIC ,
  Nm_Salesperson_Member_Id NUMERIC ,
  Nm_Owner_Parent_Acct_Id varchar(11000),
  Date_Created DATE,
  Nm_Client_Contact_Id NUMERIC,
  CV_DMA_Drop_Number NUMERIC,
  CV_Hygiene_Drop_Number NUMERIC,
  CV_Intrafile_Dups_Number NUMERIC,
  CV_Mailable_Names NUMERIC,
  CV_Nixie_Drop_Number NUMERIC,
  CV_Other_Drop_Description varchar(11000),
  CV_Other_Drop_Number NUMERIC,
  CV_Total_Drop_Number NUMERIC, 
  Exchange_Reconciled_Y_N varchar(3),
  Output_Names varchar(11000),
  Clearance_Instructions varchar(11000),
  Shipping_Method_Ref varchar(11000),
  Nm_Ship_To_Contact_Id NUMERIC,
  Ship_To_City varchar(11000),
  Ship_To_Postal_Code varchar(11000),
  Ship_To_State varchar(11000)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://{s3-axle-internal}/raw/nextmark/listorder/'
TABLE PROPERTIES ('skip.header.line.count'='1');

DROP VIEW IF EXISTS interna.nextmark_list_Order CASCADE;

CREATE VIEW interna.nextmark_list_Order 
AS 
SELECT * 
  FROM spectrumdb.nextmark_list_Order
  WITH NO SCHEMA BINDING;

--------------------
-- nextmark_inpfinanceall
--------------------
DROP TABLE IF EXISTS spectrumdb.nextmark_inpfinanceall CASCADE;

CREATE EXTERNAL TABLE spectrumdb.nextmark_inpfinanceall (
  AP_Bill_Balance_Due                NUMERIC, 
  AP_Bill_Total                      NUMERIC,
  AP_Guarantee_Prepay                VARCHAR(11000),
  AP_Paid_Amt                        NUMERIC,
  AP_Pre_Discount_Amt                NUMERIC,
  AR_Bad_Debt_Amt                    NUMERIC,
  AR_Guarantee_Prepay                VARCHAR(11000),
  AR_Invoice_Balance_Due             NUMERIC,
  AR_Invoice_Total                   NUMERIC,
  AR_Pre_Discount_Amt                NUMERIC,
  AR_Received_Amt                    NUMERIC,
  Base_Broker_Est_Comm_Rate          NUMERIC,
  Base_Commission_Rate               NUMERIC,
  Base_Commission_Units              VARCHAR(11),
  Base_Discount_Rate                 NUMERIC,
  Base_Discount_Units                VARCHAR(11),
  Base_Manager_Est_Comm_Rate         NUMERIC,
  Base_Rate                          NUMERIC,
  Base_Rate_UOM                      VARCHAR(11),
  Base_Select_Names                  VARCHAR(11000),
  Campaign_Name                      VARCHAR(11000),
  Client_Account_Name                VARCHAR(11000),
  Client_PO_Number                   VARCHAR(11000),
  Contact_Name                       VARCHAR(11000),
  Credit_Comments                    VARCHAR(11000),
  Credit_Y_N                         VARCHAR(11),
  Current_Due                        VARCHAR(11000),
  Date_Closed                        DATE,
  Date_Created                       DATE,
  Date_Due                           DATE,
  Date_Invoiced_Billed               DATE,
  Date_Mail_End                      DATE,
  Date_Mail_Start                    DATE,
  Date_Ordered_Order_Date            DATE,
  Date_Shipped                       DATE,
  Exchange_Fee                       NUMERIC,
  Finance_Type                       VARCHAR(11000),
  Invoice_Paid_Flag                  VARCHAR(11),
  Invoice_Bill_Account_Name          VARCHAR(11000),
  Invoice_Bill_Account_Role          VARCHAR(11000),
  Invoice_Bill_Number                VARCHAR(11000),
  Invoice_Bill_Status                VARCHAR(11000),
  Invoice_Bill_Type                  VARCHAR(11000),
  Key_Code                           VARCHAR(11000),
  Media_Name                         VARCHAR(11000),
  Order_Financial_Status             VARCHAR(11000),
  list_Order_Number                  VARCHAR(11000), -- it has # 
  list_Order_Profit_Center           VARCHAR(11000),
  list_Order_Profit_Center_Code      VARCHAR(11000),
  list_Order_Rent_Exch               VARCHAR(11000),
  list_Order_Status                  VARCHAR(11000),
  list_Order_Test_Cont               VARCHAR(11000),
  list_Order_Type                    VARCHAR(11000),
  Mailer_Account_Name                VARCHAR(11000),
  Net_Name_AP_percent                NUMERIC,
  Net_Name_AR_percent                NUMERIC,
  Offer_Type                         VARCHAR(11000),
  Overdue_1_To_30_Days               NUMERIC, 
  Overdue_31_To_60_Days              NUMERIC,
  Overdue_61_To_90_Days              NUMERIC,
  Overdue_91_To_120_Days             NUMERIC,
  Overdue_More_Than_120_Days         NUMERIC,
  Owner_Account_Name                 VARCHAR(11000),
  Profit                             NUMERIC,
  Profit_Center_Division             VARCHAR(11000),
  Profit_Center_Name                 VARCHAR(11000),
  Qty_Invoice_Bill_Base              NUMERIC,
  Qty_Order_Total                    NUMERIC,
  Qty_Ship_Exchange                  NUMERIC,
  Qty_Ship_Rental                    NUMERIC,
  Qty_Shipped_Total                  NUMERIC,
  Qty_Total_Useable_Names            NUMERIC,
  Rebill_Y_N                         VARCHAR(3),
  Salesperson_Full_Name              VARCHAR(11000),
  Total_Balance_Due                  NUMERIC,
  Total_Credit_Payment               NUMERIC,
  Total_Invoice_Bill_Amount          NUMERIC,
  Current_Bill_Invoice_Y_N           VARCHAR(3),
  Net_Days                           numeric,
  Nm_Account_Id                      numeric,
  Nm_Contact_Id                      numeric,
  Nmo_Invoice_Id                     numeric,
  Nm_Media_Id                        numeric,
  Nm_Order_Id                        numeric,
  Date_Finance_Object_Last_Updated   DATE,
  AP_CancelledDebt_Amt               NUMERIC,
  Invoice_Bill_Comments              VARCHAR(11000),
  Prepaid_Invoice_Bill_Y_N           VARCHAR(11),
  Client_Usage_Po                    VARCHAR(11000),
  Date_Usage_End                     date,
  Date_Usage_Start                   date,
  Nm_Uage_Id                         NUMERIC,
  Total_Tax_Amount                   NUMERIC,
  Invoice_Ship_To_Address            VARCHAR(11000),
  Invoice_Ship_To_City               VARCHAR(11000),
  Invoice_Ship_To_Postal_Code        VARCHAR(11000),
  Invoice_Ship_To_State_Code         VARCHAR(11000),
  Invoice_Ship_To_Country_Code       VARCHAR(11000)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://{s3-axle-internal}/raw/nextmark/inpfinanceall/'
TABLE PROPERTIES ('skip.header.line.count'='1');

DROP VIEW IF EXISTS interna.nextmark_inpfinanceall CASCADE;

CREATE VIEW interna.nextmark_inpfinanceall 
AS 
SELECT * 
  FROM spectrumdb.nextmark_inpfinanceall
  WITH NO SCHEMA BINDING;
 

-- These statement will generate an error if the RedShift Spectrum cannot read data from S3 source files, or has data type mismatch.
SELECT TOP 100 * 
  FROM interna.nextmark_list_Order;

SELECT TOP 100 * 
  FROM interna.nextmark_inpfinanceall;
