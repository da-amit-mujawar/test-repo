UNLOAD ( 'SELECT company_id,
       SUM(ACTV_A)          as L2_actv_voter_in_hh_count,
       SUM(GOP)             as L2_repub_in_hh_count,
       COUNT(individual_id) as L2_indiv_in_hh_count
FROM (
         SELECT company_id,
                individual_id,
                CASE WHEN l2_Parties_Description = ''REPUBLICAN'' THEN 1 ELSE 0 END AS GOP,
                CASE WHEN l2_Voters_Active = ''A'' THEN 1 ELSE 0 END                AS ACTV_A
         FROM {tablename2}
     )
WHERE len(company_id) <= 12
GROUP BY company_id ')
TO 's3://{s3-internal}{exportpath}'
IAM_ROLE '{iam}'
FORMAT PARQUET
ALLOWOVERWRITE
;