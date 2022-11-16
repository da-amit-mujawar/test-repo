/* created LEC inference  with imputation and filter and feature engg on primary_naics_code_id
and dumped in s3 along with each place type filter */
CREATE TABLE datascience.lec_inference AS (
       SELECT   infogroup_id, 
                in_business, 
                place_type, 
                location_employee_count,
                COALESCE(corporate_sales_revenue,-1) AS corporate_sales_revenue,
                primary_naics_code_id,
                primary_sic_code_id,
                branch_count,
                COALESCE(contacts_count,median(contacts_count) OVER ()) AS contacts_count,
                CAST(CASE WHEN stock_exchange_code='' THEN '0' ELSE stock_exchange_code END AS INTEGER) AS stock_exchange_code,
                COALESCE(benefit_plans_count,0) AS benefit_plans_count,
                LEFT(primary_naics_code_id,2) AS nacis_2,
                COALESCE(corporate_employee_count,-1) AS corporate_employee_count,
                CASE WHEN address_type_code='' THEN 'Unknown' ELSE address_type_code END AS address_type_code,
                LEFT(primary_naics_code_id,6) AS code_6,
                country_code,
                naics_code_ids_count,
                ucc_filings_count,
                COALESCE(population_density,median(population_density) OVER ()) AS population_density,
                LEFT(primary_naics_code_id,4) AS code_4,
                headquarters_id
        FROM core_bf.places
        WHERE in_business IN ('yes','maybe')
);

UNLOAD ('SELECT * FROM datascience.lec_inference WHERE place_type=''individual'' ')
TO 's3://{s3-datascience}/{ds_location_employee_s3_key}/location_employee_count_inference/individual_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD ('SELECT * FROM datascience.lec_inference WHERE place_type=''kiosk'' ')
TO 's3://{s3-datascience}/{ds_location_employee_s3_key}/location_employee_count_inference/kiosk_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD ('SELECT * FROM datascience.lec_inference WHERE place_type=''independent'' ')
TO 's3://{s3-datascience}/{ds_location_employee_s3_key}/location_employee_count_inference/independent_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD ('SELECT * FROM datascience.lec_inference WHERE place_type=''branch'' ')
TO 's3://{s3-datascience}/{ds_location_employee_s3_key}/location_employee_count_inference/branch_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD ('SELECT * FROM datascience.lec_inference WHERE place_type=''headquarters'' ')
TO 's3://{s3-datascience}/{ds_location_employee_s3_key}/location_employee_count_inference/headquarters_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

--drop table
DROP TABLE IF EXISTS datascience.lec_inference;