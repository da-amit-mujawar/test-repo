== view from the base table  [DMM_Report].[dbo].[NORDIC_FinanceAll]
-- Version 1.0 
-- Author: Keru Kannan 

USE [DMM_Report]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE view [dbo].[View_NORDIC_FinanceAll_DSG] As
Select
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AP_Bill_Balance_Due,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AP_Bill_Total,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AP_Guarantee_Prepay,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AP_Paid_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AP_Pre_Discount_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AR_Guarantee_Prepay,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AR_Invoice_Balance_Due,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AR_Invoice_Total,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AR_Received_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(AR_Pre_Discount_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Broker_Est_Comm_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Commission_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Commission_Units,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Discount_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Discount_Units,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Manager_Est_Comm_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Rate,''),char(13)+char(10),''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_Rate_UOM,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Base_Select_Names,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Campaign_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Client_Account_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Client_PO_Number,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Contact_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Credit_Comments,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Credit_YN,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Current_Due,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Closed,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Created,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Due,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_InvoicedBilled,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Mail_End,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Mail_Start,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Ordered,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Shipped,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exchange_Fee,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Finance_Type,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Invoice_Paid_Flag,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(InvoiceBill_Account_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(InvoiceBill_Account_Role,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(InvoiceBill_Number,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(InvoiceBill_Status,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(InvoiceBill_Type,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Key_Code,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(List_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Financial_Status,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Number,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Profit_Center,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Profit_Center_Code,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_RentExch,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Status,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_TestCont,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Type,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Mailer_Account_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Days,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_AP_PC,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_AR_PC,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Offer_Type,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Overdue_1_To_30_Days,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Overdue_31_To_60_Days,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Overdue_61_To_90_Days,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Overdue_91_To_120_Days,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Overdue_More_Than_120_Days,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Owner_Account_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Profit,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Profit_Center_Division,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Profit_Center_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_InvoiceBill_Base,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Order_Total,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Ship_Exchange,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Ship_Rental,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Shipped_Total,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Total_Useable_Names,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Rebill_YN,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Salesperson_Full_Name,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Total_Balance_Due,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Total_Credit_Payment,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Total_InvoiceBill_Amount,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Current_BillInvoice_YN,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_Account_Id,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_Contact_Id,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_Invoice_Id,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_List_Id,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_List_Order_Id,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(FORMAT(COALESCE(Date_Finance_Object_Last_Updated,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(AP_Cancelled_Debt_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Invoice_Bill_Comments,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Prepaid_Invoice_Bill_YN,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Client_Usage_PO,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(FORMAT(COALESCE(Date_Usage_Start,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(FORMAT(COALESCE(Date_Usage_End,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(NM_Usage_ID,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(AR_Bad_Debt_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Total_Tax_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(FORMAT(COALESCE(Tax_Calc_Timestamp,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_City_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_City_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_County_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_County_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_District_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_District_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_Response_Message,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_State_Amt,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_State_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_Status,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Tax_Total_Rate,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Invoice_Ship_To_Address,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Invoice_Ship_To_City,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Invoice_Ship_To_State_Code,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Invoice_Ship_To_Postal_Code,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +  
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Invoice_Ship_To_Country_Code,''),'â€“',''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"' 
 as cline from [DMM_Report].[dbo].[NORDIC_FinanceAll] where  Date_InvoicedBilled >='01/01/2016';



GO
