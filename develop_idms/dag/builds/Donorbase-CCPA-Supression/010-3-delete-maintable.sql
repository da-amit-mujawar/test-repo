UNLOAD ('SELECT ''matched_tblmain_count HHLD Level - ''||count(distinct CompanyMC)::varchar as count FROM {maintable_name}
            where CompanyMC in(select distinct CompanyMC from Adhoc_CCPA_suppression )
         UNION
         SELECT ''matched_tblmain_count IND Level - '' || count(distinct IndividualMC)::varchar as count FROM {maintable_name}
            where IndividualMC in (select distinct IndividualMC from Adhoc_CCPA_suppression)
         UNION
         SELECT ''CCPA_load_count - ''||count(*)::varchar FROM Adhoc_CCPA_suppression')
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
(CompanyMC IN (SELECT DISTINCT CompanyMC FROM Adhoc_CCPA_suppression ))
or
(IndividualMC IN (SELECT DISTINCT IndividualMC FROM Adhoc_CCPA_suppression ));