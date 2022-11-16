/*
--MGISystemProcess_Finance_Data_Extract_Report_Reduced_Column_Count.csv
DROP TABLE IF EXISTS interna.NORDIC_FinanceAll;

CREATE TABLE interna.NORDIC_FinanceAll
(
  AP_Bill_Balance_Due                VARCHAR(11000),
  AP_Bill_Total                      VARCHAR(11000),
  AP_Guarantee_Prepay                VARCHAR(11000),
  AP_Paid_Amt                        VARCHAR(11000),
  AP_Pre_Discount_Amt                VARCHAR(11000),
  AR_Bad_Debt_Amt                    VARCHAR(11000),
  AR_Guarantee_Prepay                VARCHAR(11000),
  AR_Invoice_Balance_Due             VARCHAR(11000),
  AR_Invoice_Total                   VARCHAR(11000),
  AR_Pre_Discount_Amt                VARCHAR(11000),
  AR_Received_Amt                    VARCHAR(11000),
  Base_Broker_Est_Comm_Rate          numeric,-- no fraction present
  Base_Commission_Rate               VARCHAR(11), --  it has comma in the number
  Base_Commission_Units              VARCHAR(11),
  Base_Discount_Rate                 varchar(30),
  Base_Discount_Units                VARCHAR(11),
  Base_Manager_Est_Comm_Rate         VARCHAR(11), -- it has comma in the number
  Base_Rate                          VARCHAR(11000),-- it has $ sign and comma
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
  Exchange_Fee                       varchar(11),
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
  Order_Number                       VARCHAR(11000), -- it has #
  Order_Profit_Center                VARCHAR(11000),
  Order_Profit_Center_Code           VARCHAR(11000),
  Order_Rent_Exch                    VARCHAR(11000),
  Order_Status                       VARCHAR(11000),
  Order_Test_Cont                    VARCHAR(11000),
  Order_Type                         VARCHAR(11000),
  Mailer_Account_Name                VARCHAR(11000),
  Net_Name_AP_percent                VARCHAR(11000),-- % symbol
  Net_Name_AR_percent                VARCHAR(11000),-- % symbol
  Offer_Type                         VARCHAR(11000),
  Overdue_1_To_30_Days               VARCHAR(11000),--$
  Overdue_31_To_60_Days              VARCHAR(11000),
  Overdue_61_To_90_Days              VARCHAR(11000),
  Overdue_91_To_120_Days             VARCHAR(11000),
  Overdue_More_Than_120_Days         VARCHAR(11000),
  Owner_Account_Name                 VARCHAR(11000),
  Profit                             VARCHAR(11000),-- $
  Profit_Center_Division             VARCHAR(11000),
  Profit_Center_Name                 VARCHAR(11000),
  Qty_Invoice_Bill_Base              VARCHAR(11000),
  Qty_Order_Total                    varchar(30), --number with fraction comma present
  Qty_Ship_Exchange                  VARCHAR(11),
  Qty_Ship_Rental                    VARCHAR(11000),
  Qty_Shipped_Total                  VARCHAR(11000),
  Qty_Total_Useable_Names            VARCHAR(11000),
  Rebill_Y_N                         VARCHAR(3),
  Salesperson_Full_Name              VARCHAR(11000),
  Total_Balance_Due                  VARCHAR(11000), -- $ sign present
  Total_Credit_Payment               VARCHAR(11000),
  Total_Invoice_Bill_Amount          VARCHAR(11000),
  Current_Bill_Invoice_Y_N           VARCHAR(3),
  Net_Days                           numeric,
  Nm_Account_Id                      numeric,
  Nm_Contact_Id                      numeric,
  Nmo_Invoice_Id                     numeric,
  Nm_Media_Id                        numeric,
  Nm_Order_Id                        numeric,
  Date_Finance_Object_Last_Updated   DATE,
  AP_CancelledDebt_Amt               VARCHAR(11000),--$ present
  Invoice_Bill_Comments              VARCHAR(11000),
  Prepaid_Invoice_Bill_Y_N           VARCHAR(11),
  Client_Usage_Po                    VARCHAR(11000),
  Date_Usage_End                     date,
  Date_Usage_Start                   date,
  Nm_Uage_Id                         NUMERIC,
  Total_Tax_Amount                   VARCHAR(11000),
  Invoice_Ship_To_Address            VARCHAR(11000),
  Invoice_Ship_To_City               VARCHAR(11000),
  Invoice_Ship_To_Postal_Code        VARCHAR(11000),
  Invoice_Ship_To_State_Code         VARCHAR(11000),
  Invoice_Ship_To_Country_Code       VARCHAR(11000)
);

COPY interna.NORDIC_FinanceAll
FROM 's3://{s3-axle-internal}/bronze_nextmark/MGISystemProcess_Finance_Data_Extract_Report_Reduced_Column_Count.csv'
IAM_ROLE '{iam}'
DELIMITER ',' CSV
QUOTE '"'
IGNOREHEADER 1
DATEFORMAT AS 'MM/DD/YYYY';

DROP TABLE IF EXISTS interna.NORDIC_ListOrder;
CREATE TABLE interna.NORDIC_ListOrder
(
  Base_Actual_Rate_Units varchar(3),
  Base_AP_Rate varchar(11000), -- $ present in number
  Base_AR_Rate varchar(38), -- it has comma number
  Base_Broker_Est_Comm_Rate NUMERIC ,
  Base_Commission_Rate varchar(38), -- it has comma number 
  Base_Commission_Units varchar(11000),
  Base_Discount_Rate varchar(38), -- it has comma number
  Base_Discount_Units varchar(3),
  Base_Manager_Est_Comm_Rate varchar(38), -- it has comma number
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
  Exch_Broker_Run_Chg_Max_Amt NUMERIC ,
  Exch_Broker_Run_Chg_Min_Amt NUMERIC ,
  Exch_Manage_Run_Chg_Max_Amt NUMERIC ,
  Exch_Manage_Run_Chg_Min_Amt varchar(38),-- comma present in the number
  Exchange_Broker_Run_Charge_Rate varchar(11000),-- $ present in number
  Exchange_Broker_Run_Charge_Units varchar(3),
  Exchange_Manager_Run_Charge_Rate varchar(11000),-- $ present in number
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
  Net_Name_AP_percent varchar(11000),
  Net_Name_AP_Run_Charge_Rate varchar(11000),
  Net_Name_AR_percent varchar(11000),
  Net_Name_AR_Run_Charge_Rate varchar(11000),
  Net_Name_Run_Charge_Units varchar(11000),
  Net_Name_Run_Chg_Comm_Rate varchar(11000),
  Net_Name_Run_Chg_Comm_Units varchar(11000),
  Net_Name_Run_Chg_Disc_Rate varchar(11000),
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
  Output_AP_Rate_per_F varchar(11000), --$ present in the number
  Output_AP_Rate_per_M varchar(11000), --$ present in the number
  Output_AR_Rate_per_F varchar(11000), --$ present in the number
  Output_AR_Rate_per_M varchar(11000), --$ present in the number
  Output_Commission_Rate_per_F varchar(11000), --$ present in the number
  Output_Commission_Rate_per_M varchar(11000), --$ present in the number
  Output_Discount_Rate_per_F varchar(11000), --$ present in the number
  Output_Discount_Rate_per_M varchar(11000), --$ present in the number
  Owner_Account_Name varchar(11000),
  Owner_Parent_Account varchar(11000),
  Owner_SB_Account_Name varchar(11000),
  Preclearance_Y_n varchar(3),
  Profit_Center_Code numeric(38,2) ,
  Profit_Center_Division varchar(11000),
  Profit_Center_Name varchar(11000),
  Qty_at_Full_Rate varchar(11000),
  Qty_at_Run_Rate varchar(11000),
  Qty_Available varchar(30),--comma present in the number 
  Qty_Order_Exchange varchar(30),--comma present in the number
  Qty_Order_Rental varchar(11000), --comma present in the number
  Qty_Order_Total varchar(11000),  --comma present in the number
  Qty_Rental_Useable_Names varchar(11000),
  Qty_Shipi_Exchange varchar(30),--comma present in the number 
  Qty_Ship_Rental varchar(11000),
  Qty_Ship_Total varchar(11000),
  Qty_Total_Useable_Names varchar(11000),
  Qty_Usage_to_Date varchar(30),--comma present in the number
  Quantity_Rule varchar(11000),
  Reuse_Type varchar(11000),
  Salesperson_Alias varchar(11000),
  Salesperson_Full_Name varchar(11000),
  Select_AP_Rate_per_F varchar(11000),--$ present in the number
  Select_AP_Rate_per_M varchar(11000), --$ present in the number
  Select_AR_Rate_M varchar(11000), --$ present in the number
  Select_AR_Rate_per_F varchar(11000),--$ present in the number
  Select_Commission_Rate_per_F varchar(11000), --$ present in the number
  Select_Commission_Rate_per_M varchar(11000),  --$ present in the number
  Select_Discount_Rate_per_F varchar(11000),--$ present in the number
  Select_Discount_Rate_per_M varchar(11000),
  Select_Names varchar(11000),
  Ship_Label varchar(11000),
  Ship_To_Contact_Name varchar(11000),
  Shipping_Amount varchar(11000),
  Shipping_Method varchar(11000),
  Tax_Amount varchar(11000),
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
  Date_Created varchar(11000),
  Nm_Client_Contact_Id NUMERIC ,
  CV_DMA_Drop_Number varchar(30), -- comma present in the number
  CV_Hygiene_Drop_Number varchar(30),-- comma present in the number
  CV_Intrafile_Dups_Number varchar(30),-- comma present in the number 
  CV_Mailable_Names varchar(11000),
  CV_Nixie_Drop_Number varchar(30),-- comma present in the number 
  CV_Other_Drop_Description varchar(11000),
  CV_Other_Drop_Number varchar(30),-- comma present in the number 
  CV_Total_Drop_Number varchar(30),-- comma present in the number 
  Exchange_Reconciled_Y_N varchar(11000),
  Output_Names varchar(11000),
  Clearance_Instructions varchar(11000),
  Shipping_Method_Ref varchar(11000),
  Nm_Ship_To_Contact_Id NUMERIC 
);


COPY interna.NORDIC_ListOrder
FROM 's3://{s3-axle-internal}/bronze_nextmark/MGISystemProcess_List_Order_Data_Extract_Report_.csv'
IAM_ROLE '{iam}'
DELIMITER ',' CSV
QUOTE '"'
IGNOREHEADER 1
DATEFORMAT AS 'MM/DD/YYYY'
*/
