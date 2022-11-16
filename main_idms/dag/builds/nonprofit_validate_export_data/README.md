# NonProfit Validation dag for apogee and donorbase
    - Validates NonProfit list conversions by ownerid for a build at status 120 
        - Input is on Athena DW_Final_ files
        - Processes lists at status 120, bypasses status 125 and 140 
        - Updates Exclude_ListConversion_Audit table
        - Outputs to s3 buckets for db 
    

# DAG runtime arguments:
    - for eg:- {"databaseid":1438,"buildid":23927,"ownerid":0,"exporttransaction":"Y"}
               {"databaseid":74,"buildid":24579,"ownerid":0,"exporttransaction":"Y"}
    - databaseid: int
    - buildid: int
    - ownerid: int Use O to process all lists
    - exporttransaction: char (Y/N)
        Y for normal runs, generates export files on s3 for data science team 
        N for testing by ownerid, or testing build runs, will NOT generate export files.

## Post DAG: 
**Run NonProfit-Pivot-ListConversion-Audit-Report**

    - Runtime Config eg: {"databaseid":1438}
                     eg: {"databaseid":74}
    - Report generated from Exclude_ListConversion_Audit table for a build


