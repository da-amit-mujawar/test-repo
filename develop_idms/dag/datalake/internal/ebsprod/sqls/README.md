This Folder contains sql scripts that are used for generating objects in both Oracle and Redshift databases:

create_oracle_datalake_schema_objects.sql
-- This script creates the datalake schema user in Oracle and the objects (Table with Metadata and Procedures needed by the extract scripts)
create_redshift_objects.sql
-- This script is used to drop and create spectrumdb schema tables and their corresponding interna schema views in the redshift cluster DB.   
create_redshift_objects_with_counts.sql
-- This script is used to drop and create spectrumdb schema tables and their corresponding interna schema views in the redshift cluster DB.   
-- This script includes count and top10 verify sqls included with it. 
get_redshift_tab_count.sql
-- The query in this script is used by extract_table.sh script for getting count from Redshift interna.view and updating Oracle metadata table. 
-- This is then used to validate counts between Oracle and Redshift tables. 
validate_extract.sql
-- The query in this script is used by run_extract.sh script to validate counts between oracle and redshift. 
extract_report.sql
-- The query in this script is used by run_extract.sh script to send report in email after daily extracts is done. 
