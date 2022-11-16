# *DATA-AXLE - Apogee Build ETL Pipeline*

- Build Apogee Database (74)  weekly transactions, monthly build
- Airflow job location: /idms/dag/builds/apogee/

### Setup

#### **Variables**
        
        - var-db-74: contains the databaseid, name and build related information for Apogee for Convert. The variable
                    is freshly populated when the etl job is triggered.
        
### Custom functions
        - helper/apogee.py

### Steps
#### apogee-a-preaudit
     - apogee-a-preaudit/apogee-preaudit.py - Dag - *apogee-a-pre-audit-process*
        - Task: fetch_buildinfo - Fetch the most recent build information for apogee for convert. 
        
        - Task: get_input_count
               - This task calls apogee_input_count() in apogee.py. 
               - This function replace sql server sp: DW_ListLoad_02.dbo.usp_GetValueFromBuildList.
               - This task replaces VC job: APOGEE-A2  VALIDATE DONOR & TRANS INPUT FILES (tblChild3 source)
    
                - The logic first checks all the lists in current update. It checks for following problems:
                    1. Idendify lists that don't have the corresponding donor or transaction list.  The lists should
                        be in pairs.
                    2. Lists with different actions between the donor and transaction list.
                - If no issues identified, it will perform the following counts using apogee list final tables
                in Redshift for generating report:
                    1. Total donor file record count
                    2. total transaction file record count
                    3. Distinct donor count
                    4. Non-empty company_id record count
                    5. Matching records count between donor and transaction files
                - If above mentioned issues encountered, the task produce an error, stops and send error email.
                - count result is stored in Redshift table: apogee_input_count 
                Note: **apogee_input_count table is dropped everytime the task is run.  We might consider
                renaming it to keep historical data.**
        
        - Task: get_donor_trans_count
            - This task calls apogee_donortrans_count() in apogee.py. This function replaces sql server sp: DW_ListLoad_02.dbo.usp_Apogee_PreUpdateAudit_DonTransCounts
            - This task replaces VC job: APOGEE-A3  PRE-UPDATE AUDIT REPORT - LIST SUMMARY
            - It performs the following counts for generating report:
                1. Minimum donation date
                2. Maximum donation date
                3. Donor count
                4. Transaction count
            - The counts are stored in Redshift table: apogee_donortrans_count
    
        - Task: get_preaudit_report
            - This task calls apogee_preaudit_report() function in apogee.py
            - The task combines (union) counts in apogee_input_count and apogee_donortrans_count tables
            - pivot on count type to generate pre-audit report
            - report is created and saved in airflow sever: /tmp/Pre-Update-Audit-for-BuildID-{buildid}-yyyy-mm-dd
            - report location and name is saved into var-db-74 varable with key = 'audit-report-name'
  
         - Task: email_notification
            - Send out notification emails
            - Attached pre-audit-report

#### apogee-b-load-transactions & validations
    - prospector-load.py - dag *apogee-b1-load-transcations*
    - replaces VC job APOGEE-B1  LOAD TRANSCATIONS - NONPROFIT Donations (tblChild3)
    - replaces VC job APOGEE-B2  LOAD TRANSCATIONS - FEC Donations (tblChild4)
    
    - tasks:
        - fetch_buildinfo_apogee_conv
        
        - fetch_buildinfo_apogee
        
        - get_fec_nonfec_list: calls get_np_fec_list()
            1. Get non-profit and fec donor and transaction list
            2. store list information in Redshift table: apogee_list_values
        
        - load_np_transactions: calls load_np_transactions()
            1. get non-profit donor and transaction lists
            3. loop through each donor/transaction list pair
            3. perform dedupe logic on donor records
            4. perform dedupe logic on transaction records
            5. join deduped donor and transaction table to dedupe again by ListID, AccountNo, Detail_DonationDate, Detail_DonationDollar
            6. insert records into final non-profit transaction table: apogee-transaction-np
            7. if action ='A', fetch records for the list from iq child3 table (hardcoded for now) and insert into apogee-transaction-np
            (Note that in apogee Add is not update and add, It is just increamental add to existing records.
            8. To do: Need to get records for lists with No action.  Jayesh is going to review the existing logic and check
            if dedupe logic is required.
            9. To do: Need to take care of Delete/Remove actions
            
                   
        - update_np_transaction_category: calls update_category()
            1. create a category temp table based on ListCategoryFull field in sql_tblMasterLoL table
            2. update list category fields in apogee-transaction-np table 
        
        - load_fec_transactions: calls load_fec_transactions()
            1. get fec lists
            2. loop through each fec donor/transaction list pair
            3. perform dedupe logic on fec donor records
            4. perform dedupe logic on fec transaction records
            5. join deduped fec donor and transaction tables to dedupe again by ListID, AccountNo, Detail_DonationDate, Detail_DonationDollar
            6. insert records into final fec transaction table: apogee-transaction-fec
            7. if action ='A', fetch records for the list from iq child4 table (hardcoded for now) and insert into apogee-transaction-fec
    
        - cleanup_transaction
            - remove invalid records from np and fec transaction tables
                - invalid date
                - future date
                - aged date
                - negative or 0 donation
                - records suppressed: apogee_tblSuppression (sql table, need to be moved to Redshift)
                
        - update list categories for non-profit transactions
        
    ** Note ** 
    load_l2_ref and update_l2_ind_comp_ids tasks are commented out. 
    If these tasks need to be performed, make sure sure the files exported from
    sql server are present on the s3 location specified in config.json.  If the reference
    tables are already in Redshift, the sql scripts are not going to be needed.
