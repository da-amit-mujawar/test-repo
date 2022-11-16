/* this query recalculates the branch count by splitting the ancestor_headquarters_ids 
and taking count of infogroup_id along each splitted ancestor_headquarters_ids */
DROP TABLE IF EXISTS datascience.temp_branch_count;
CREATE TABLE datascience.temp_branch_count AS
(with NS AS (
  SELECT 1 AS n UNION ALL
  SELECT 2 UNION ALL
  SELECT 3 UNION ALL
  SELECT 4 UNION ALL
  SELECT 5 UNION ALL
  SELECT 6 UNION ALL
  SELECT 7 UNION ALL
  SELECT 8 UNION ALL
  SELECT 9 UNION ALL
  SELECT 10
)
SELECT 
  TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n)) AS infogroup_id
  ,CAST(COUNT(infogroup_id) AS INTEGER) AS new_branch_count
  FROM NS
  INNER JOIN 
    core_bf.places B 
  ON NS.n <= REGEXP_COUNT(B.ancestor_headquarters_ids, ',') + 1
  WHERE SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n) <> '' 
  AND in_business='yes'
  GROUP BY SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n)
);

/* this query calculates branch location employee count by splitting the ancestor_headquarters_ids 
and taking sum of location_employee_count along each splitted ancestor_headquarters_ids */
DROP TABLE IF EXISTS datascience.temp_new_location_feature;
CREATE TABLE datascience.temp_new_location_feature AS
(with NS AS (
  SELECT 1 AS n UNION ALL
  SELECT 2 UNION ALL
  SELECT 3 UNION ALL
  SELECT 4 UNION ALL
  SELECT 5 UNION ALL
  SELECT 6 UNION ALL
  SELECT 7 UNION ALL
  SELECT 8 UNION ALL
  SELECT 9 UNION ALL
  SELECT 10
)
SELECT 
  TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n)) AS infogroup_id
  ,CAST(SUM(location_employee_count) AS INTEGER) AS branch_location_employee_count
  FROM NS
  INNER JOIN 
    core_bf.places B 
  ON NS.n <= REGEXP_COUNT(B.ancestor_headquarters_ids, ',') + 1
  WHERE SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n) <> '' 
  AND in_business='yes'
  GROUP BY SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n)
);
/* this query determines those headquarters whose subsidiary is not present in the data, 
or they  don’t have a  subsidiaries headquarter */

DROP TABLE IF EXISTS datascience.temp_hq_not_contains_infogroup_id;
CREATE TABLE datascience.temp_hq_not_contains_infogroup_id AS
(SELECT 
  a.infogroup_id   
  FROM (
          SELECT infogroup_id FROM core_bf.places 
          WHERE in_business='yes' AND place_type='headquarters' AND headquarters_id =''
        ) a 
LEFT JOIN 
       (   
          SELECT  headquarters_id FROM core_bf.places 
          WHERE in_business='yes' AND place_type='headquarters' AND headquarters_id<>''
          group by headquarters_id
        ) b 
ON a.infogroup_id=b.headquarters_id 
WHERE b.headquarters_id IS NULL
);

--create final headquarters_without_subsidiary table
DROP TABLE IF EXISTS datascience.headquarters_without_subsidiary;
CREATE TABLE datascience.headquarters_without_subsidiary AS
(select b.state,
       b.tags_count,
       b.primary_sic_code_id,
       b.primary_naics_code_id,
       b.corporate_sales_revenue,
       b.population_density,
       b.contacts_count,
       b.stock_exchange_code,
       b.country_code,
       b.has_ecommerce,
       b.csa_code,
       b.ucc_filings_count,
       b.zip_four,
       b.benefit_plans_count,
       b.latitude,
       a.infogroup_id,
       b.in_business,
       b.ancestor_headquarters_ids,
       b.branch_count,
       b.place_type,
       b.headquarters_id,
       b.ultimate_headquarters_id,
       b.location_employee_count,
       b.corporate_employee_count,
       b.number_of_tenants,
       b.website,
       b.facebook_url,
       b.twitter_url,
       b.linked_in_url,
       b.yelp_url,
       b.pinterest_url,
       b.youtube_url,
       b.tumblr_url,
       b.foursquare_url,
       b.instagram_url,
       b.logo_url, 
       c.new_branch_count,
       d.branch_location_employee_count
 FROM datascience.temp_hq_not_contains_infogroup_id a
 LEFT JOIN 
    core_bf.places b ON a.infogroup_id = b.infogroup_id
 LEFT JOIN 
     datascience.temp_branch_count c ON a.infogroup_id = c.infogroup_id
 LEFT JOIN 
     datascience.temp_new_location_feature d ON a.infogroup_id = d.infogroup_id
 WHERE b.in_business='yes' AND b.place_type='headquarters'
);

/* this query determines Headquarters whose subsidiary headquarter is present in the data */
DROP TABLE IF EXISTS datascience.temp_hq_contains_infogroup_id;
CREATE TABLE datascience.temp_hq_contains_infogroup_id AS
(SELECT a.infogroup_id 
 FROM (
        SELECT infogroup_id FROM core_bf.places 
        WHERE in_business='yes' AND place_type='headquarters' AND headquarters_id =''
      ) a 
 LEFT JOIN 
     (   
        SELECT headquarters_id FROM core_bf.places 
        WHERE in_business='yes' AND place_type='headquarters' AND headquarters_id<>''
        group by headquarters_id
      ) b 
 ON a.infogroup_id=b.headquarters_id 
 WHERE b.headquarters_id IS NOT NULL
);

--create final headquarters_with_subsidiary table
DROP TABLE IF EXISTS datascience.headquarters_with_subsidiary;
CREATE TABLE datascience.headquarters_with_subsidiary AS
(SELECT b.state,
        b.tags_count,
        b.primary_sic_code_id,
        b.primary_naics_code_id,
        b.corporate_sales_revenue,
        b.population_density,
        b.contacts_count,
        b.stock_exchange_code,
        b.country_code,
        b.has_ecommerce,
        b.csa_code,
        b.ucc_filings_count,
        b.zip_four,
        b.benefit_plans_count,
        b.latitude,
        a.infogroup_id,
        b.in_business,
        b.ancestor_headquarters_ids,
        b.branch_count,
        b.place_type,
        b.headquarters_id,
        b.ultimate_headquarters_id,
        b.location_employee_count,
        b.corporate_employee_count,
        b.number_of_tenants,
        b.website,
        b.facebook_url,
        b.twitter_url,
        b.linked_in_url,
        b.yelp_url,
        b.pinterest_url,
        b.youtube_url,
        b.tumblr_url,
        b.foursquare_url,
        b.instagram_url,
        b.logo_url, 
        c.new_branch_count,
        d.branch_location_employee_count
FROM datascience.temp_hq_contains_infogroup_id a
LEFT JOIN 
    core_bf.places b ON a.infogroup_id = b.infogroup_id
LEFT JOIN 
     datascience.temp_branch_count c ON a.infogroup_id = c.infogroup_id
LEFT JOIN 
     datascience.temp_new_location_feature d ON a.infogroup_id = d.infogroup_id
WHERE b.in_business='yes' AND b.place_type='headquarters'
);

/* this query determines those subsidiary headquarters who has a parent headquarter in the data */
DROP TABLE IF EXISTS datascience.temp_infogroup_id_contains_hq;
CREATE TABLE datascience.temp_infogroup_id_contains_hq AS
(SELECT infogroup_id  
 FROM core_bf.places 
 WHERE in_business='yes' AND place_type='headquarters' AND headquarters_id<>'' AND headquarters_id in 
    (   SELECT infogroup_id FROM core_bf.places 
        WHERE in_business='yes' AND place_type='headquarters' AND headquarters_id<>''
    )
);

--create final subsidiary_with_headquarters table
DROP TABLE IF EXISTS datascience.subsidiary_with_headquarters;
CREATE TABLE datascience.subsidiary_with_headquarters AS
(SELECT b.state,
        b.tags_count,
        b.primary_sic_code_id,
        b.primary_naics_code_id,
        b.corporate_sales_revenue,
        b.population_density,
        b.contacts_count,
        b.stock_exchange_code,
        b.country_code,
        b.has_ecommerce,
        b.csa_code,
        b.ucc_filings_count,
        b.zip_four,
        b.benefit_plans_count,
        b.latitude,
        a.infogroup_id,
        b.in_business,
        b.ancestor_headquarters_ids,
        b.branch_count,
        b.place_type,
        b.headquarters_id,
        b.ultimate_headquarters_id,
        b.location_employee_count,
        b.corporate_employee_count,
        b.number_of_tenants,
        b.website,
        b.facebook_url,
        b.twitter_url,
        b.linked_in_url,
        b.yelp_url,
        b.pinterest_url,
        b.youtube_url,
        b.tumblr_url,
        b.foursquare_url,
        b.instagram_url,
        b.logo_url, 
        c.new_branch_count,
        d.branch_location_employee_count
 FROM datascience.temp_hq_contains_infogroup_id a
 LEFT JOIN 
    core_bf.places b ON a.infogroup_id = b.infogroup_id
 LEFT JOIN 
     datascience.temp_branch_count c ON a.infogroup_id = c.infogroup_id
 LEFT JOIN 
     datascience.temp_new_location_feature d ON a.infogroup_id = d.infogroup_id
 WHERE b.in_business='yes' AND b.place_type='headquarters'
);

--dump headquarters_without_subsidiary data to s3
UNLOAD ('SELECT  * from datascience.headquarters_without_subsidiary')
TO 's3://{s3-datascience}/{ds_corporate_s3_key}/headquarters_without_subsidiary_'  
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

--dump headquarters_with_subsidiary data to s3
UNLOAD ('select  * from datascience.headquarters_with_subsidiary')
TO 's3://{s3-datascience}/{ds_corporate_s3_key}/headquarters_with_subsidiary_'  
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 

--dump subsidiary_with_headquarters data to s3
UNLOAD ('select  * from datascience.subsidiary_with_headquarters')
TO 's3://{s3-datascience}/{ds_corporate_s3_key}/subsidiary_with_headquarters_'   
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 