# *DATA-AXLE - PROSPECTOR ETL PIPELINE*

- Build Prospector Database based on data from its member databases
- Airflow job location: /idms/dag/builds/prospector/

### Setup

#### **Variables**

        - var-db-1094: contains the databaseid, name and build related information. The variable
            is freshly populated when the etl job is triggered.
            
        - var-db-1094-fieldlist: contains the list of base fields, datatype, field width for prospector database.
            This variable is used in tasks to generate field specifications in create table and select queries.


### Components

    - prospector-load.py - dag
    
    - config.json - contains custom email subject and messages
    
    - helper/sqlserver.py
    
    - get_build_info() for obtaining the inactive build information. 
    
    - activate_build() to activate the build.
    
    - helper/prospector.py - prospector_load_table() 
        
        - Contains the bulk of the logic for loading prospector's member databases into prospector database.
            It first fetch the list of databases to be included in prospector database for the current build, 
            then loop through each database to gather records from the latest tblmain table, and followed 
            by inserting these records into prospector's tblmain table.
        
        - Loading errors are tracked in prospector_load_error_log table in Redshift.  Most loading errors are related
            to incorrect column width and missing columns that are required.
        
        - Once all databases are processed, it checks to see if there is any loading errors.  Raise an error if there is
            to stop the pipeline. An excel report for the loading error is also created and the path to the report is inserted
            into the var-db-1094 with the key of 'load_error_report'.  If needed, the report can be added to a notification
            task later.
    
    - transformation queries: sql files prefixed with 001, 002 and 003.  The logic is lifted and moved from Prospector
      VC job. These queries are only executed when there is no loading error found in previous task.
      
        - 001_add_additional_calculated_fields.sql
        - 002_add_recency_calculated_fields.sql
        - 003_add_ind_hh_recency_fields.sql

### Tasks in the Dag
    - fetch_buildinfo: Load inactive build info into var-db-1094 for tasks to use
    
    - load_main_table: Load data into Prospector main table
    
        Please note: The task will try to load all databases even if there are errors.  After the
        full load, it will error out so it does not proceed to the next task.  Error encountered
        are tracked in prospector_looad_error_log tabe.
    
    - transform_task: Call sql queries to perform transformation and add additional fields
    
    - get_count: Count the total records in Prospetor tblMain after loading
    
    - activate: Compare minimum count to the cound returned from get_count task. Change status to
        70 (waiting for activation) if returned count is greater than mininum count required.

    - send_notification: Send email.

### Trouble Shooting
    - Loading errors due to missing table, column, column size difference are tracked in 
      prospector_load_error_log in Redshift. 
    
    - Dag errors out if there is any error registered in the above mentioned table. An email notification
      should be sent to idmsadminnotification.

    - Review the error logs and coordinate with list admins to fix any issues encountered.
    
    - If data load completed without error, it will run the task of activating build by changing
      the build status to 70 (waiting for activation)
