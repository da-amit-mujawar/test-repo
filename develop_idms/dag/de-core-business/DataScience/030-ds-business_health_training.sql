/*dump business helath traning data by left joining places with bankruptcies*/

UNLOAD ('SELECT  a.facebook_url, 
                 a.twitter_url, 
                 a.instagram_url, 
                 a.Website ,
                 a.has_ecommerce, 
                 a.branch_count ,
                 a.in_business_on, 
                 a.work_at_home ,
                 a.business_type_ids_count, 
                 a.sic_code_ids_count ,
                 a.ucc_filings_count ,
                 a.brand_ids_count ,
                 a.cbsa_level ,
                 a.place_type ,
                 a.nbrc_corporation_filing_type, 
                 a.phone_call_status_code ,
                 a.contacts_count ,
                 a.primary_naics_code_id, 
                 a.in_business ,
                 a.infogroup_id,
                 b.dismissal 
        FROM core_bf.places a
        LEFT JOIN core_bf.bankruptcies b ON a.infogroup_id=b.infogroup_id
        WHERE ((a.in_business=''no'' AND a.in_business_on ='''') OR (a.in_business=''yes'' and a.in_business_on!=''''))
        AND (a.branch_count>=0 OR a.branch_count is NULL)
        AND a.primary_naics_code_id <> '''' ' )
TO 's3://{s3-datascience}/{ds_business_health_s3_key}/business_health_training_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 