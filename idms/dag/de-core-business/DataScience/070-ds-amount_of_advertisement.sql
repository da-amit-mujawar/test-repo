--created independent table
DROP TABLE IF EXISTS datascience.independent;
CREATE TABLE datascience.independent AS (
 SELECT infogroup_id,
		in_business_on,
		primary_sic_code_id,
		sic_code_ids_count,
		primary_naics_code_id,
		naics_code_ids_count,
		corporate_employee_count,
		corporate_sales_revenue,
		branch_count,
		eins_count,
		has_ecommerce,
		population_density,
		contacts_count,
		location_employee_count,
		bulk_updated_at,
		express_updated_at,
		location_intents_count,
		happy_hours_count,
		corporate_franchising
 FROM core_bf.places
 WHERE in_business='yes' AND place_type='independent'
);


--created headquarters table
DROP TABLE IF EXISTS datascience.headquarters;
CREATE TABLE datascience.headquarters AS (
 SELECT infogroup_id, 
		in_business_on, 
		primary_sic_code_id,
		sic_code_ids_count, 
		primary_naics_code_id, 
		naics_code_ids_count,
		corporate_employee_count, 
		corporate_sales_revenue, 
		branch_count,
		eins_count, 
		has_ecommerce, 
		population_density, 
		contacts_count,
		location_employee_count, 
		bulk_updated_at, 
		express_updated_at,
		location_intents_count, 
		happy_hours_count, 
		corporate_franchising,
		estimated_corporate_employee_count,
		estimated_corporate_sales_revenue
 FROM core_bf.places
 WHERE in_business='yes' AND place_type='headquarters'
);

--created branch table
DROP TABLE IF EXISTS datascience.branch;
CREATE TABLE datascience.branch AS (
 SELECT infogroup_id,
		in_business_on,
		primary_sic_code_id,
		sic_code_ids_count,
		primary_naics_code_id,
		naics_code_ids_count,
		corporate_employee_count,
		corporate_sales_revenue,
		branch_count,
		eins_count,
		has_ecommerce,
		population_density,
		contacts_count,
		location_employee_count,
		bulk_updated_at,
		express_updated_at,
		location_intents_count,
		happy_hours_count,
		corporate_franchising
 FROM core_bf.places
 WHERE in_business='yes' AND place_type='branch'
);

--dump headquarters data
UNLOAD ('select * from datascience.headquarters')
TO 's3://{s3-datascience}/{ds_amount_of_adv_s3_key}/headquarters_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

--dump independent data
UNLOAD ('select * from datascience.independent')
TO 's3://{s3-datascience}/{ds_amount_of_adv_s3_key}/independent_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

--dump branch data
UNLOAD ('select * from datascience.branch')
TO 's3://{s3-datascience}/{ds_amount_of_adv_s3_key}/branch_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 