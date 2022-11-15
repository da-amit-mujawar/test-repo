unload ('select  * from EXCLUDE_TitleToCode_lookup where cTitle in (select cTitle  from EXCLUDE_TitleToCode_lookup limit 100)')
to 's3://idms-2722-internalfiles/Reports/TitleToCode_lookup_report_{{ params.reportdate }}_'
iam_role '{{ params.iamrole }}'
kms_key_id '{{ params.kmskey }}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;COMMIT;
