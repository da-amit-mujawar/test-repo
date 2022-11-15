# *DATA-AXLE - Apogee Promotion Data ETL Pipeline*

- prepare apogee promation data for data science team
- promotion history for 5 years
- promotion data are appended with individual_id and household_id
- final data are in Redshift and can also be exported to S3 location in parquet format
- airflow job location: /idms/dag/de-apogee/apogee-promotion-data/

### Setup and Dependencies
##### - Databrick jobs that load daily promotion data into delta lake database
    Jobs:
    - Promotions_splitrecaps_bronze
        Production URL: https://dbc-cacb962d-262b.cloud.databricks.com/?o=8025360762383568#job/5182
    - Promotions_splitrecaps_silver  
        Production URL: https://dbc-cacb962d-262b.cloud.databricks.com/?o=8025360762383568#job/5184
    - Promotions_mailfiles_bronze 
        Production URL: https://dbc-cacb962d-262b.cloud.databricks.com/?o=8025360762383568#job/4571
    - Promotions_mailfiles_silver 
        Production URL: https://dbc-cacb962d-262b.cloud.databricks.com/?o=8025360762383568#job/5183
    
    Delta table location (development):
    - s3://de-2722-promotions-silver/promotions_splitrecaps
    - s3://de-2722-promotions-silver/promotions_mailfiles
    
    Delta table location (production):
    - s3://nessy-apogee/dev/silver/promotions_splitrecaps
    - s3://nessy-apogee/dev/silver/promotions_mailfiles

##### - Manifest files created on updated delta tables in S3 location (development environment)
    - s3://de-2722-promotions-silver/promotions_splitrecaps/_symlink_format_manifest/'
    - s3://de-2722-promotions-silver/promotions_mailfiles/_symlink_format_manifest/
    
##### - Manifest files created on updated delta tables in S3 location (production environment)    
    note: that production files are still partitioned by client_id
		
	- s3://nessy-apogee/dev/silver/promotions_mailfiles/_symlink_format_manifest/
	- s3://nessy-apogee/dev/silver/promotions_splitrecaps/_symlink_format_manifest/

##### - Core service process setup 
    
    - to automatically fetch files from this S3 location: s3://idms-2722-aop-input/  with files prefixed with 'a03'
    - to perform aop address hygiene process and append individual_id and household_id fields on matching records
    - to output the matching records with appended fields to s3://idms-2722-aop-output/ 
    - returned file should be prefixed with 'a03', containing header and is pipe delimited
    - refer to Apogee_Promotion_AIP_process.docx for aop setup
    - job id: AIPDONOR1 JobID: 1045095206
    
### Process - flow
    - create Redshift external tables for mailfiles and splitrecaps using manifest files from delta lake database created in Databrick jobs
  
    - load new mailfiles into Redshift table using external table

    - load new splitrecaps into Redshift table using external table

    - output records that are new for aop process

    - load files returned from aop proccess to Redshift

    - update Redshift mailfiles table using returned aop records

    - export final promotion data to s3 if needed

### Jobs

#####apogee-promotion-load-to-redshift.py
    - tasks:
        - create-apogee-promotion-s3-external-tables: use delta table manifest file to create external table in Redshift
            - 100-create-external-schema-database.sql
            - 101-create-external-promotion-splitrecaps.sql
            - 102-create-external-promotion-mailfiles.sql
        - load-apogee-promo-mailfiles: use external table to load data into Redshift mailfiles table
            - 110-load-mailfiles.sql
        - load-apogee-promo-splitrecaps: use external table to load new data into Redshift splitrecaps table
            - 120-load-splitrecaps.sql
        - output-for-aop: export file for aop to process
            - 150 unload-for-aop.sql

#####apogee-promotion-update-redshift-output
    - tasks:
        - aop_update
            - load file returned from aop process
            - update mailfile table 
            - 300-load-aop-output.sql
            
        - output_file
            - join mailfiles with splitrecap for output to s3
            - 900-promotion-output.sql

#####Note: refer to apogee_promo_dataissue_call_notes.docx for more details.  

### Reference :
    JIRA: https://infogroup.atlassian.net/browse/APOGEE-36

###TO DO:###
- 
- Currently the Silver job takes 9+ Hrs to process. This needs to be revisited to tune the performance
- 