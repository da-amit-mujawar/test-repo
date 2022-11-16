/* Dump square footage training with imputation and filter and feature engg on primary_naics_code_id*/
UNLOAD('SELECT  a.infogroup_id,
                a.in_business,
                a.place_type,
                a.cbsa_code,
                CAST(CASE WHEN a.census_tract ='''' THEN ''-1'' ELSE census_tract END AS INTEGER) AS census_tract,
                a.city,
                a.county_code,
                COALESCE(a.contacts_count,median(a.contacts_count) OVER ()) AS contacts_count,
                LEFT(a.primary_naics_code_id,2) AS nacis_2,
                COALESCE(a.number_of_tenants,0) AS number_of_tenants,
                COALESCE(a.population_density,median(a.population_density) OVER ()) AS population_density,
                a.postal_code,
                a.primary_sic_code_id,
                a.location_employee_count,
                a.corporate_employee_count,
                a.branch_count,
                a.location_parent_relationship,
                a.location_parent_id,
                a.has_ecommerce,
                a.website,
                b.prop_unvbldsqft,
                CASE WHEN CAST(b.prop_unvbldsqft AS INTEGER)>=0 AND CAST(b.prop_unvbldsqft AS INTEGER) <1500 THEN 1 
                     WHEN CAST(b.prop_unvbldsqft AS INTEGER) >=1500 AND CAST(b.prop_unvbldsqft AS INTEGER) <2500 THEN 2
                     WHEN CAST(b.prop_unvbldsqft AS INTEGER) >=2500 AND CAST(b.prop_unvbldsqft AS INTEGER) <5000 THEN 3
                     WHEN CAST(b.prop_unvbldsqft AS INTEGER) >=5000 AND CAST(b.prop_unvbldsqft AS INTEGER) <10000 THEN 4
                     WHEN CAST(b.prop_unvbldsqft AS INTEGER) >=10000 AND CAST(b.prop_unvbldsqft AS INTEGER) <20000 THEN 5 
                     WHEN CAST(b.prop_unvbldsqft AS INTEGER) >=20000 AND CAST(b.prop_unvbldsqft AS INTEGER) <40000 THEN 6 
                     WHEN CAST(b.prop_unvbldsqft AS INTEGER) >=40000 AND CAST(b.prop_unvbldsqft AS INTEGER) <100000 THEN 7
                     WHEN CAST(b.prop_unvbldsqft AS INTEGER) >=100000 THEN 8 ELSE 0 END AS target_class
      FROM core_bf.places a
      INNER JOIN core_bf.infutor b ON a.infogroup_id=b.infogroup_id
      WHERE in_business IN (''yes'',''maybe'') AND b.prop_unvbldsqft!=''''
      AND place_type NOT IN (''kiosk'',''individual'') ')
TO 's3://{s3-datascience}/{ds_square_footage_s3_key}/square_footage_training_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

