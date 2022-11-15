/* dump business health inference data*/
UNLOAD ('SELECT  facebook_url, 
                 twitter_url, 
                 instagram_url, 
                 Website ,
                 has_ecommerce, 
                 branch_count ,
                 in_business_on, 
                 work_at_home ,
                 business_type_ids_count, 
                 sic_code_ids_count ,
                 ucc_filings_count ,
                 brand_ids_count ,
                 cbsa_level ,
                 place_type ,
                 nbrc_corporation_filing_type, 
                 phone_call_status_code ,
                 contacts_count ,
                 primary_naics_code_id, 
                 in_business ,
                 infogroup_id 
        FROM core_bf.places
        WHERE ((in_business=''no'' AND in_business_on ='''') OR (in_business=''yes'' and in_business_on!=''''))
        AND (branch_count>=0 OR branch_count is NULL)
        AND primary_naics_code_id <> ''''

        UNION ALL
 

        SELECT   facebook_url, 
                 twitter_url, 
                 instagram_url, 
                 Website ,
                 has_ecommerce, 
                 branch_count ,
                 in_business_on, 
                 work_at_home ,
                 business_type_ids_count, 
                 sic_code_ids_count ,
                 ucc_filings_count ,
                 brand_ids_count ,
                 cbsa_level ,
                 place_type ,
                 nbrc_corporation_filing_type, 
                 phone_call_status_code ,
                 contacts_count ,
                 primary_naics_code_id, 
                 in_business ,
                 infogroup_id 
        FROM core_bf.places
        WHERE in_business NOT IN (''no'',''yes'',''junk'')
        AND (branch_count>=0 OR branch_count is NULL)
        AND primary_naics_code_id <>'''' ' )
TO 's3://{s3-datascience}/{ds_business_health_s3_key}/business_health_inference_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 