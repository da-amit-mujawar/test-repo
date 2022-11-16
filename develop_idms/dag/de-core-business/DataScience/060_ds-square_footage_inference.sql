/* created square footage inference with imputation and filter and feature engg on primary_naics_code_id
and dumped in s3 along with each place type filter */
DROP TABLE IF EXISTS datascience.square_footage_inference;
CREATE TABLE datascience.square_footage_inference AS (
       SELECT infogroup_id,
              in_business,
              place_type,
              COALESCE(population_density,median(population_density) OVER ()) AS population_density,
              postal_code,
              CAST(CASE WHEN census_tract ='' THEN '-1' ELSE census_tract END AS INTEGER) AS census_tract,
              city,
              COALESCE(number_of_tenants,0) AS number_of_tenants,
              COALESCE(contacts_count,median(contacts_count) OVER ()) AS contacts_count,
              county_code,
              LEFT(primary_naics_code_id,2) AS nacis_2,
              primary_sic_code_id,
              cbsa_code,
              square_footage
       FROM core_bf.places WHERE in_business IN ('yes','maybe')
);

UNLOAD('SELECT *
        FROM datascience.square_footage_inference
        WHERE place_type=''individual'' ')
to 's3://{s3-datascience}/{ds_square_footage_s3_key}/square_footage_inference/individual_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD('SELECT *
        FROM datascience.square_footage_inference
        WHERE place_type=''kiosk'' ')
to 's3://{s3-datascience}/{ds_square_footage_s3_key}/square_footage_inference/kiosk_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD('SELECT *
        FROM datascience.square_footage_inference
        WHERE place_type=''branch'' ')
to 's3://{s3-datascience}/{ds_square_footage_s3_key}/square_footage_inference/branch_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD('SELECT *
        FROM datascience.square_footage_inference
        WHERE place_type=''headquarters'' ')
to 's3://{s3-datascience}/{ds_square_footage_s3_key}/square_footage_inference/headquarters_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

UNLOAD('SELECT *
        FROM datascience.square_footage_inference
        WHERE place_type=''independent'' ')
to 's3://{s3-datascience}/{ds_square_footage_s3_key}/square_footage_inference/independent_'
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 


