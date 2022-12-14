# *Data-Axle - Summary Calculation Pipeline for Apogee, Apogee FEC, and Donorbase*

## Config Files:
* ###**input_layout.json**
  - Input layout based on incoming data structure. Has two sections. Records and Filters
  - Records - List of columns - Input Structure 
    - Process_ID column is required and used as the key column in the process 
    - E.g.
    
          {
            "Name": "Process_ID",
            "Datatype": "String",
            "ColNo": 4,
            "Original_Column_Name": "Company_ID",
            "Comments": "Original Column name is Company ID. Renamed as Process ID"
          }    
      
  - Filter - Filter any bad records or the records to discard
    - E.g.
    
          {
            "Name": "Valid_Company",
            "Formula": " AND (Process_ID != '0' AND Process_ID IS NOT NULL)"
          }    
    
* ###**PrefixA.json**
  - Contains the Duration { Recency in Months [12, 24, 48 and Lifetime]}
  - E.g.

        {
             "Code": "A1",
             "Value": "M12",
             "Formula": "MB_DonationDate BETWEEN 0 AND 12"
        },
        {
             "Code": "A2",
             "Value": "M48",
             "Formula": "MB_DonationDate BETWEEN 0 AND 48"
        },

  
* ###**PrefixB.json**
  - Contains Category, and Types
  - E.g.
    
        {
             "Code": "B2",
             "Value": "CAT_TOT",
             "Formula": ""
        },
        {
             "Code": "B5",
             "Value": "LTD_Months_Since_Last_Donation",
             "Comments": "Used Min since this is used on calculated field -- MB_DonationDate is  Months Between DonationDate",
             "Formula": "MIN"
        }, 
  - 

* ###**PrefixC.json**
  - Contains Category Level, List ID, or Months and column names
  - E.g.

        {
             "Code": "C1",
             "Value": "_<CATEGORY>",
             "Formula": " <CATEGORY> in (ListCategory01, ListCategory02) "
        },
        {
             "Code": "C2",
             "Value": "_<LISTID>",
             "Formula": " ListID = <LISTID> "
        },
        {
             "Code": "C3",
             "Value": "_<MONTHS>",
             "Formula": " SUM(CASE WHEN date_format(to_date(DonationDate),'MMMM') = '<MONTHS>' THEN 1 ELSE 0 END)"
        }

* ###**PrefixD.json**
  - Contains All Field level formula and column names
  - E.g.
    
        {
             "Code": "D1",
             "Value": "AVG_Dollar_Donation",
             "Formula": "ROUND(AVG(CASE WHEN <C_FORMULA> THEN DonationDollar ELSE null END ),0)"
        },
        {
             "Code": "D2",
             "Value": "Dollar_Donation",
             "Formula": "SUM(CASE WHEN <C_FORMULA> THEN DonationDollar ELSE 0 END )"
        }

- 
* ###**Domain.json**
  - To maintain the Data Types - Not Used in PySpark
   
* ###**Combinations.json**
  - Maintains the combination of PrefixA, PrefixB, PrefixC, and PrefixD  
  - E.g.
    
        {  
             "A": "A2",
             "B": "B2",
             "C": "C1",
             "D": "D2",
             "E": "E1"
        },

* ###_**Note:**_ 
  * **PrefixD.json**: The key column should be defined with Formula "ID". This will be used for all the groupings in the process.
    -     {
               "Code": "D15",
               "Value": "Process_ID",
               "Formula": "ID"
          }
  * **Combinations.json**: The key column should be defined as follows.
    -     {
               "A": "A0",
               "B": "B0",
               "C": "C0"
               "D": "D15"
               "E": "E3"
          }
  
    - 
* ###**Sample queries**
  * Here is few sample queries generated by the process. You can see all the queries using EMR Logs. 
  * The column name is generated using value column from PrefixX.json files.  
  -
  
          SELECT Process_ID AS Process_ID ,   
                 CEIL(months_between(to_date('20220527','yyyyMMdd') + 5, MAX(DonationDate))) AS M48_CAT_AN_Months_Since_Last_Donation , 
                 CEIL(months_between(to_date('20220527','yyyyMMdd') + 5, MAX(DonationDate))) AS M48_CAT_TOT_AN_Months_Since_Last_Donation,
                 ROUND(AVG(CASE WHEN  'AN' IN (ListCategory01, ListCategory02)  THEN DonationDollar ELSE null END ),0) AS M48_CAT_AN_AVG_Dollar_Donation, 
                 ROUND(AVG(CASE WHEN  'AN' IN (ListCategory01, ListCategory02)  THEN DonationDollar ELSE null END ),0) AS M48_CAT_TOT_AN_AVG_Dollar_Donation, 
                 SUM(CASE WHEN  'AN' IN (ListCategory01, ListCategory02)  THEN 1 ELSE 0 END ) AS M48_CAT_AN_Number_Of_Donation ,  
                 SUM(CASE WHEN  'AN' IN (ListCategory01, ListCategory02)  THEN 1 ELSE 0 END ) AS M48_CAT_TOT_AN_Number_Of_Donation,  
                 SUM(CASE WHEN  'AN' IN (ListCategory01, ListCategory02)  THEN DonationDollar ELSE 0 END ) AS M48_CAT_AN_Dollar_Donation ,
                 SUM(CASE WHEN  'AN' IN (ListCategory01, ListCategory02)  THEN DonationDollar ELSE 0 END ) AS M48_CAT_TOT_AN_Dollar_Donation 
            FROM NP_TransCat
            WHERE MB_DonationDate <= 48 AND 'AN' in (ListCategory01, ListCategory02)
         GROUP BY Process_ID

* ###**CustomQueries.json**
  - Use the CustomQueries.json to create additional columns if needed by adding custom queries
  - Need to add additional Task in the DAG to enable this feature.
  - "MetaData" - List of PySparkTables 
    
          {  
               "MetaData": {
                   "Raw Files": {
                       "TableName": "NP_Transactions",
                       "KeyColumnName": "Process_ID"
                   },
                   "TblLTDData": {
                       "TableName": "tblLTD",
                       "KeyColumnName": "Company_ID"
                   }
  - "Records" - List of Columns to be used for Custom Queries
    
          "Records": [
            {
              "Code": "CST01",
              "ID": "1",
              "Query": "SELECT DISTINCT Process_ID, CAST( FIRST_VALUE(PaymentMethod) OVER (PARTITION BY Process_ID ORDER BY DonationDate DESC) AS CHAR) LTD_Last_Payment_Method   FROM NP_Transactions"
            },
            {
              "Code": "CST01",
              "ID": "4",
              "Query": "SELECT Company_ID as Process_ID, CASE WHEN LTD_Average_Donation_Per_Transaction < 10 THEN 'A'  WHEN LTD_Average_Donation_Per_Transaction < 25 THEN 'B' WHEN LTD_Average_Donation_Per_Transaction < 50 THEN 'C' WHEN LTD_Average_Donation_Per_Transaction < 100 THEN 'D' WHEN LTD_Average_Donation_Per_Transaction < 500 THEN 'E' ELSE 'F' END AS LTD_AVG_Donation_Range FROM tblLTD"
            },
          ]


* ##**Locations**
  - PySpark Code is available in s3 location s3://idms-7933-prod-code/not-for-profit/
  - All the above-mentioned configuration files to be available in s3://idms-7933-prod-code/not-for-profit/config/**&lt;applname&gt;** folder
  - **&lt;applname&gt;** can be apogee or apogee-fec or donorbase   
  - Any additional SQL files that needs to be executed will need to be placed under `idms/dag/builds/apogee/apogee-summary/dag/<applname>/sql/` directory
    - SQL Files has to start with 100. E.g `100_001_create-seq-gen-id.sql`
    - These SQL scripts will be executed on RedShift directly and not on PySpark SQL. 
    
* # **DAG**
  - All the DAGs are created under `idms/dag/builds/apogee/apogee-summary/dag/<applname>` directory
  - There will be three DAGs for each process
  - **EMR Process-** `builds-summary-calc-<ApplicationName>-EMR`
    - Loads the data from the incoming files to S3 using three process.
       1. **Convert_CSV_2_Parquet** - Converts CSV/Incoming files to parquet format in S3
       2. **Prepare_Input_Data** - Prepare the input data and creates the following tables
          1. NP_Transactions - Transactional Data
          2. NP_TransCat - Transactional Data Partitioned by ListCategory
          3. NP_TransList - Transactional Data Partitioned by ListID
       3. **Process_Calc**
          1. The process is sub-divided into five parts
             1. ProcessLTD - Base data calculation 
             2. ProcessCategory - Category Level Calculation
             3. ProcessMonths - Month Level Calculation
             4. ProcessList - List Level Calculation
             5. ProcessCustomAttributes - Custom Attributes Calculation based on [CustomQueries.json](#CustomQueries.json) file
    - The final output data will be stored in S3 location [] along with SQL scripts generated at []  
        
  - **DB Process-** `builds-summary-calc-<ApplicationName>-DB`
    - Pulls the data generated by EMR Process from S3 and load the data into RedShift Database.
    - The process is divided into 
       1. **Download_SQL_Files_from_s3** - Process to download all the SQL files from S3 to local and split the files based on number of SQL statements
       2. **Redshift_Load_ProcessData** - Loads the data from Parquet to RedShift based on SQL scripts downloaded from S3
       3. **Redshift_Load_CustomData** - Loads any additional scripts that needs to be executed. This step runs only if there is any additional .sql scripts are placed into sql folder
       4. 
  - **Consolidated** `builds-summary-calc-<ApplicationName>-All` 
    - This DAG runs as a combined process of `builds-summary-calc-<ApplicationName>-EMR` and `builds-summary-calc-<ApplicationName>-DB`
    - No Intervention is required. 

## **Support**
  - **DAG** 
    - `builds-summary-calc-<ApplicationName>-All` :
      - This process is not scheduled and supposed to be trigerred manually to run the end to end process
      - Incase of any failure
        - Check if the EMR steps are complete. If so, Kick off `builds-summary-calc-<ApplicationName>-DB` DAG to run DB process alone
        - If EMR Process is not complete, try to identify the root cause of failure and restart the DAG
  - **Error Messages**

  - 
        Error:
            ERROR - Exception: Error while Creating EMR Cluster
            INFO - Task exited with return code 1
        Cause:
            Error on creating EMR Cluster due to several reasons.
              1. Access to create EMR Cluster
              2. EMR Cluster is terminated by user.  
  
  - 
        Error
            botocore.errorfactory.NoSuchKey: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
        Cause:
            Authentication Issue. Check the authorization key
            
  - 
        Error
            ERROR - Rejects exceeded 0.05%. Halting process!!!.
        Cause:
            Too many bad records on the incoming file. 
        Fix:
            Fix the incoming and rerun the process.
  
  - 
        Error
            ERROR - Write DDL Script to S3 failed.
        Cause:
            Process not able to write the DDL scripts to S3 using Boto3. 
        Fix:
            Grant access and re-run the process.


