Deployment Steps:

-- SQL server side deployment 

1.DMM_Report_View_NORDIC_ListOrder_DSG.sql
2.DMM_Report_View_NORDIC_FinanceAll_DSG.sql

schedule to run from sqlserver side
1.Tablelist.txt
2.DSG_Extracts_Nextmark.cmd
3.DSG_s3_filecopy.cmd

deploy from redshift side

1.Redshift_External_Views_ListOrder.sql
2.Redshift_External_Views_FinanceAll.sql
