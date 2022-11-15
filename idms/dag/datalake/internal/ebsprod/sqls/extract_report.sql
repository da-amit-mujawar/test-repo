set TERMOUT off;
set feedback off;
set heading on;
set echo off
set linesize 200 trimspool on
SET MARKUP HTML ON SPOOL ON
set pagesize 50000;
alter session set  NLS_DATE_FORMAT='MM-DD-YYYY HH24:MI';
spool &1..htm
select schema_name||'_'||table_name Extracted_Table, Start_Time, End_Time, extract_type, status, 
extract_rows_count extract_delta_count,
redshift_pre_load_count target_pre_extract_count, 
table_rows_count source_rows_count, 
redshift_post_load_count target_rows_count ,
abs(table_rows_count - redshift_post_load_count) Count_Diff,
case when abs(table_rows_count - redshift_post_load_count) >= round(table_rows_count*.01) Then 'mismatch' Else 'ok' End as Result
from source_extracts
;
spool off
set markup html off
exit

