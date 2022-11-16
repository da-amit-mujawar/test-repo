-- Update Commands used to manually
--       Update the from/to date as needed.

update datalake.source_extracts set incremental_condition = 'trunc(creation_date) between trunc(sysdate - 2) and trunc(sysdate - 1) or trunc(last_update_date) between trunc(sysdate - 2) and trunc(sysdate - 1)'
where incremental_condition = 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)';

-- Use this to reset them back.
update datalake.source_extracts set incremental_condition = 'trunc(last_update_date) between trunc(sysdate - 2) and trunc(sysdate - 1)'
where incremental_condition = 'trunc(last_update_date) = trunc(sysdate - 1)';
