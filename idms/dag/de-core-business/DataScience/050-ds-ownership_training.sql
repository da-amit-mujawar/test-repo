UNLOAD ( 'SELECT a.infogroup_id,
                a.in_business_on,
                a.place_type,
                a.mailing_address,
                a.address_type_code,
                a.number_of_tenants,
                a.cbsa_level,
                a.primary_naics_code_id,
                a.location_owns_location AS "location.owns_location",
                CASE WHEN a.location_owns_location=''true'' THEN 1
                ELSE 0 END outcome
        FROM core_bf.places a
        LEFT JOIN core_bf.infutor b ON a.infogroup_id = b.infogroup_id
        WHERE a.in_business in (''yes'',''maybe'') AND 
              a.place_type NOT IN (''individual'',''kiosk'') AND 
              a.work_at_home<>''true'' AND
              a.cmra_flag<>''true''  AND 
              a.location_owns_location IS NOT NULL AND
              a.primary_naics_code_id <> '''' AND
              (b.prop_ownerocc IS NULL OR  b.prop_ownerocc <> ''O'')

        UNION ALL

        SELECT  a.infogroup_id,
                a.in_business_on,
                a.place_type,
                a.mailing_address,
                a.address_type_code,
                a.number_of_tenants,
                a.cbsa_level,
                a.primary_naics_code_id,
                a.location_owns_location AS "location.owns_location",
                CASE WHEN a.location_owns_location=''true'' THEN 1
                ELSE 0 END outcome
        FROM core_bf.places a
        LEFT JOIN core_bf.infutor b ON a.infogroup_id = b.infogroup_id
        WHERE a.in_business in (''yes'',''maybe'') AND 
              a.place_type NOT IN (''individual'',''kiosk'') AND 
              a.work_at_home<>''true'' AND
              a.cmra_flag<>''true''  AND 
              a.primary_naics_code_id <> '''' AND
              b.prop_ownerocc = ''O'' and a.location_owns_location=''true'' ')
TO 's3://{s3-datascience}/{ds_ownership_s3_key}/ownership_training_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 