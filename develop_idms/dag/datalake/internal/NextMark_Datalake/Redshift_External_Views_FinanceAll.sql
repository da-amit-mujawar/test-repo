DROP TABLE IF EXISTS spectrumdb.inpFinanceAll;
CREATE EXTERNAL TABLE spectrumdb.inpFinanceAll(
AP_Bill_Balance_Due numeric,
AP_Bill_Total numeric,
AP_Guarantee_Prepay varchar(2000) ,  
AP_Paid_Amt numeric, 
AP_Pre_Discount_Amt numeric,  
AR_Guarantee_Prepay varchar(2000),
AR_Invoice_Balance_Due numeric, 
AR_Invoice_Total numeric,
AR_Received_Amt numeric,
AR_Pre_Discount_Amt numeric,
Base_Broker_Est_Comm_Rate numeric, 
Base_Commission_Rate numeric,
Base_Commission_Units varchar(200),
Base_Discount_Rate numeric,
Base_Discount_Units varchar(30),
Base_Manager_Est_Comm_Rate numeric,  
Base_Rate numeric,
Base_Rate_UOM varchar(3),
Base_Select_Names text,
Campaign_Name varchar(3000),
Client_Account_Name varchar(3000),  
Client_PO_Number varchar(3000),
Contact_Name varchar(3000),
Credit_Comments varchar(3000),
Credit_YN varchar(3),
Current_Due numeric,
Date_Closed timestamp,
Date_Created timestamp, 
Date_Due timestamp,
Date_InvoicedBilled timestamp,  
Date_Mail_End timestamp,
Date_Mail_Start timestamp,
Date_Ordered timestamp,
Date_Shipped timestamp,
Exchange_Fee numeric,
Finance_Type varchar(30),
Invoice_Paid_Flag varchar(3),
InvoiceBill_Account_Name varchar(2000),  
InvoiceBill_Account_Role varchar(2000),
InvoiceBill_Number varchar(300),
InvoiceBill_Status varchar(30),  
InvoiceBill_Type varchar(1000),
Key_Code varchar(100),
List_Name varchar(100),
List_Order_Financial_Status varchar(30),
List_Order_Number varchar(300),  
List_Order_Profit_Center varchar(300),  
List_Order_Profit_Center_Code varchar(300),
List_Order_RentExch varchar(300),  
List_Order_Status varchar(30),  
List_Order_TestCont varchar(30),
List_Order_Type varchar(300),  
Mailer_Account_Name varchar(300),
Net_Days numeric,  
Net_Name_AP_PC varchar(300),  
Net_Name_AR_PC varchar(300),
Offer_Type varchar(300),
Overdue_1_To_30_Days numeric,
Overdue_31_To_60_Days numeric,  
Overdue_61_To_90_Days numeric,
Overdue_91_To_120_Days numeric,
Overdue_More_Than_120_Days numeric,
Owner_Account_Name varchar(300),
Profit numeric,
Profit_Center_Division varchar(200),
Profit_Center_Name varchar(200),
Qty_InvoiceBill_Base numeric,
Qty_Order_Total numeric,
Qty_Ship_Exchange numeric,
Qty_Ship_Rental varchar(200),
Qty_Shipped_Total numeric,
Qty_Total_Useable_Names varchar(200),  
Rebill_YN varchar(3),
Salesperson_Full_Name varchar(300),  
Total_Balance_Due numeric,
Total_Credit_Payment numeric,  
Total_InvoiceBill_Amount numeric,
Current_BillInvoice_YN varchar(3),
Nm_Account_Id numeric,
Nm_Contact_Id numeric,  
Nm_Invoice_Id numeric,
Nm_List_Id numeric, 
Nm_List_Order_Id numeric,
Date_Finance_Object_Last_Updated timestamp,  
AP_Cancelled_Debt_Amt numeric,
Invoice_Bill_Comments varchar(300),
Prepaid_Invoice_Bill_YN varchar(10),  
Client_Usage_PO varchar(200),
Date_Usage_Start timestamp,
Date_Usage_End timestamp,
NM_Usage_ID  varchar(300),
AR_Bad_Debt_Amt varchar(30), 
Total_Tax_Amt numeric,
Tax_Calc_Timestamp timestamp,  
Tax_City_Amt numeric,
Tax_City_Rate numeric,
Tax_County_Amt numeric,
Tax_County_Rate numeric,
Tax_District_Amt numeric,
Tax_District_Rate numeric,
Tax_Response_Message varchar(200),  
Tax_State_Amt numeric,
Tax_State_Rate numeric,
Tax_Status varchar(200),
Tax_Total_Rate numeric,
Invoice_Ship_To_Address varchar(200),  
Invoice_Ship_To_City varchar(200),
Invoice_Ship_To_State_Code varchar(200),  
Invoice_Ship_To_Postal_Code varchar(200),
Invoice_Ship_To_Country_Code varchar(200)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'acceptinvchars'='',
   'quoteChar' = '"',
   'timeformat'='auto'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/nextmark/inpfinanceall'
TABLE PROPERTIES ('skip.header.line.count'='0');
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.inpFinanceAll;

CREATE VIEW interna.inpFinanceAll 
AS 
SELECT * 
  FROM spectrumdb.inpFinanceAll
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) 
  FROM interna.inpFinanceAll;

SELECT TOP 100 * 
  FROM interna.inpFinanceAll;
