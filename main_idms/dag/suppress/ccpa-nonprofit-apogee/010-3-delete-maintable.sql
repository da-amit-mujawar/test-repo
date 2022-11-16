UNLOAD ('SELECT ''matched_tblmain_count HHLD Level - ''||count(distinct company_mc)::varchar as count FROM {maintable_name}
            where company_mc in (select distinct company_mc from exclude_ccpa_suppression )
         UNION
         SELECT ''matched_tblmain_count IND Level - '' || count(distinct individual_mc)::varchar as count FROM {maintable_name}
            where individual_mc in (select distinct individual_mc from exclude_ccpa_suppression)
         UNION
         SELECT ''CCPA_load_count - ''||count(*)::varchar FROM exclude_ccpa_suppression')
TO 's3://{s3-internal}{reportname}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;

DELETE FROM {maintable_name} 
WHERE
(company_mc IN (SELECT DISTINCT company_mc FROM exclude_ccpa_suppression ))
or
(individual_mc IN (SELECT DISTINCT individual_mc FROM exclude_ccpa_suppression )) ;