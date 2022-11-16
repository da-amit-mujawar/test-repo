UNLOAD ('SELECT headquarters_id, 
                infogroup_id, 
                estimated_location_employee_count,
                estimated_corporate_sales_revenue, 
                square_footage, 
                place_type, 
                zip
         FROM core_bf.places
         WHERE place_type NOT IN (''kiosk'', ''independent'',''individual'') AND 
               in_business NOT IN (''no'',''maybe'',''junk'')')
TO 's3://{s3-datascience}/{ds_location_sales_vol_s3_key}/location_sales_volume_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 