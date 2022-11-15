UNLOAD ('SELECT  a.place_type ,
                 a.number_of_tenants ,
                 a.cbsa_level ,
                 a.infogroup_id ,
                 a.primary_naics_code_id ,
                 a.address_type_code ,
                 a.in_business_on ,
                 a.mailing_address
         FROM core_bf.places a
         LEFT JOIN core_bf.infutor b ON a.infogroup_id=b.infogroup_id
         WHERE a.in_business IN (''yes'',''maybe'') AND 
               a.place_type NOT IN (''individual'',''kiosk'') AND 
               a.work_at_home <> ''true'' AND
               a.cmra_flag <> ''true'' AND a.primary_naics_code_id <> ''''
               AND (b.prop_ownerocc IS NULL OR  b.prop_ownerocc <> ''O'')
      
         UNION ALL

         SELECT  a.place_type ,
                 a.number_of_tenants ,
                 a.cbsa_level ,
                 a.infogroup_id ,
                 a.primary_naics_code_id ,
                 a.address_type_code ,
                 a.in_business_on ,
                 a.mailing_address
         FROM core_bf.places a
         LEFT JOIN core_bf.infutor b ON a.infogroup_id=b.infogroup_id
         WHERE a.in_business IN (''yes'',''maybe'') AND 
               a.place_type NOT IN (''individual'',''kiosk'') AND 
               a.work_at_home <> ''true'' AND
               a.cmra_flag <> ''true'' AND a.primary_naics_code_id <> ''''
               AND b.prop_ownerocc = ''O'' and (a.location_owns_location is NULL
               OR a.location_owns_location=''true'')')
TO 's3://{s3-datascience}/{ds_ownership_s3_key}/ownership_inference_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 