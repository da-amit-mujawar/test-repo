REM
col env form a5
col month form a5
col host form a35 wrap
col prerun_time form a20
col postrun_time form a20
col collection_type form a20
col prerun_data form a20 wrap
col postrun_data form a20 wrap
col result form a8
set linesize 200
set pagesize 50000;
set echo on
set heading on;
set verify on
set feedback on
select schema_name||'_'||table_name Extracted_Table, Start_Time, End_Time, extract_type, status, 
extract_rows_count extract_delta_count,
 redshift_pre_load_count target_pre_extract_count, 
table_rows_count source_rows_cnt, 
redshift_post_load_count target_rows_cnt ,
case when table_rows_count <> redshift_post_load_count Then 'mismatch' Else 'ok' End as Result
from datalake.source_extracts
;
exit;
