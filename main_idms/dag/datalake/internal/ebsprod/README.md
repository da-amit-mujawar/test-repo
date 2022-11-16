EBSPROD Datalake Ingestion Scripts:

The bash shell scripts in here used for extracting ebsprod tables and ingesting to datalake (s3 bucket).

set_env.sh 		
	Sets the common variables and functions used by the other scripts below.

extract_table.sh
	The purpose of this script is:
	   Mainly used within the main run_extracts.sh script to accomplish below tasks on a given ebsprod table/view.
 	 	Extract the table data from ebsprod Oracle database to a file.
	 	Validate for UTF8 encoding.
         	GZIP the file and copy to s3 bucket.
	

run_extracts.sh
	The purpose of this script is:
	    Retrieve all the tables from metadata table that needed to be extracted.
	    Extracts data for each table calling the extract_table.sh script
	    Validates the counts between Oracle and Redshift databases and sends a report on completion.

sqls:
	This folder contains required sqls scripts:
	    To create objects that above bash scripts depend on; &
	    Query scripts that are executed within above bash scripts for validating and sending reports.


How to schedule the daily extracts with these scripts:

1. Created datalake schema and required objects using this script in Oracle EBSPROD DB.
	create_oracle_datalake_schema_objects.sql 
2. Create target schema objects on Redshift Cluster in Prod or Dev:
	create_redshift_objects.sql
3. Schedule the run_extracts.sh using crontab for applmgr user:
 	# Run the Datalake Ingestion process for EBS Tables
	# 10 17 * * *  . ~/.bash_profile; /home/applmgr/datalake/ebs/run_extracts.sh >> /home/applmgr/datalake/ebs/log/cron_run_extracts.log


