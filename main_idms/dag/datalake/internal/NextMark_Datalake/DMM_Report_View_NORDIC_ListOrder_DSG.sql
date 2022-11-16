== view from the base table  [DMM_Report].[dbo].[NORDIC_ListOrder]
-- Version 1.0 
-- Author: Keru Kannan 

USE [DMM_Report]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[View_NORDIC_ListOrder_DSG] As Select
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Base_Actual_Rate_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Base_AP_Rate,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Base_AR_Rate,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Base_Broker_Est_Comm_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Base_Commission_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Base_Commission_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Base_Discount_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Base_Discount_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Base_Manager_Est_Comm_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Base_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Base_Select_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Base_Select_Names_wRate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Campaign_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Cancellation_Notes,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Cancellation_Status,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Clearance_Ref_No,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Clearance_Status,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Cleared_By,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Client_Account_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Client_Contact_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Client_Credit_Approval_Notes,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Client_Credit_Approved_By,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Client_Credit_Required,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Client_Credit_Status,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Client_PO_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Contact_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Cancelled,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Cleared,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Client_Credit_Approved,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Created,''),'yyyy-MM-dd HH:mm:ss'),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Data_Receipt_Cutoff,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Data_Received,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_First_Usage,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Last_Updated,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Last_Usage,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_LO_Object_Last_Updated,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Mail_End,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Mail_Start,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Needed_By,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Ordered,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Revised,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(FORMAT(COALESCE(Date_Shipped,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exch_Broker_Run_Chg_Max_Amt,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exch_Broker_Run_Chg_Min_Amt,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exch_Manage_Run_Chg_Max_Amt,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exch_Manage_Run_Chg_Min_Amt,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exchange_Broker_Run_Charge_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exchange_Broker_Run_Charge_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exchange_Manager_Run_Charge_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Exchange_Manager_Run_Charge_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(For_Review_Flag,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Key_Code,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(List_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_PC_Paid,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_PC_Received,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Financial_Status,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(List_Order_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_RentExch,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Status,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_TestCont,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(List_Order_Type,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Mailer_Account_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Mailer_SB_Account_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Manager_Account_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Mgr_Contact_Profit_Center_Code,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Mgr_Contact_Profit_Center_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_AP_PC,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_AP_Run_Charge_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_AR_PC,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_AR_Run_Charge_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_Run_Charge_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_Run_Chg_Comm_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_Run_Chg_Comm_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_Run_Chg_Disc_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Net_Name_Run_Chg_Disc_Units,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Number_Clickthroughs,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Number_Delivered,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Number_Opens,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Number_Orders,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Number_Responses,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Number_Who_Clicked,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Offer_Description,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Offer_Type,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Omit_Names,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(replace(Replace(replace(replace(replace(COALESCE(Output_AP_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Output_AP_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Output_AR_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Output_AR_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Output_Commission_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Output_Commission_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Output_Discount_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Output_Discount_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Owner_Account_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Owner_Exch_Ship_Bal,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Owner_Parent_Account,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Owner_SB_Account_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Preclearance_Yn,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Profit_Center_Code,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Profit_Center_Division,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Profit_Center_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_at_Full_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_at_Run_Rate,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Available,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Order_Exchange,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Order_Rental,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Order_Total,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Rental_Useable_Names,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Ship_Exchange,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Ship_Rental,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Ship_Total,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Total_Useable_Names,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Qty_Usage_to_Date,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Quantity_Rule,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Reuse_Type,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Salesperson_Alias,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Salesperson_Full_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_AP_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_AP_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_AR_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_AR_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_Commission_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_Commission_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_Discount_Rate_per_F,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Select_Discount_Rate_per_M,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Select_Names,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(Replace(replace(replace(COALESCE(Ship_Label,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Ship_To_Contact_Name,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(Replace(replace(replace(COALESCE(Shipping_Amount,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Shipping_Method,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Tax_Amount,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Vendor_Credit_Status,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_Campaign_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_Client_Account_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_List_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_List_Order_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_Mailer_Account_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_Mailer_Serv_Bur_Acct_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_Manager_Account_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(Nm_Owner_Acct_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_Owner_Sb_Account_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_Salesperson_Member_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_Owner_Parent_Acct_Id,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(FORMAT(COALESCE(xCreated_Date,''),'yyyy-MM-dd HH:mm:ss'),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Nm_Client_Contact_ID,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_DMA_Drop_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_Hygiene_Drop_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_Intrafile_Dups_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_Mailable_Names,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_Nixie_Drop_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_Other_Drop_Description,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_Other_Drop_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(CV_Total_Drop_Number,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Exchange_Reconciled_YN,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Output_Names,''),char(44),''),'â€“',''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Clearance_Instructions,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Shipping_Method_Ref,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(NM_Ship_To_Contact_ID,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Ship_To_City,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Ship_To_Postal_Code,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"|' +  
'"' + RTRIM(Replace(Replace(Replace(replace(replace(replace(replace(COALESCE(Ship_To_State,''),'â€“',''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|','')) + '"' 
 as cline1
from [DMM_Report].[dbo].[NORDIC_ListOrder] where Date_Created >= '1/1/2016';

GO
