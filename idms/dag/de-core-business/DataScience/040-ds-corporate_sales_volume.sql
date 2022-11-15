/*splitting and exploding 'labels_title_codes' columns of places and count no. of each values of that columns */
DROP TABLE IF EXISTS datascience.merge_contacts;
CREATE TABLE datascience.merge_contacts AS (
WITH ten_numbers AS (SELECT 1 AS num UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION 
SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 0)
, generated_numbers AS (
    SELECT (1000 * t1.num) + (100 * t2.num) + (10 * t3.num) + t4.num AS gen_num
    FROM ten_numbers AS t1
    JOIN ten_numbers AS t2 ON 1 = 1
    JOIN ten_numbers AS t3 ON 1 = 1
    JOIN ten_numbers AS t4 ON 1 = 1
 )
, splitter AS (
SELECT *
FROM generated_numbers
WHERE gen_num BETWEEN 1 AND (SELECT MAX(REGEXP_COUNT(labels_title_codes, '\\,') + 1)
FROM core_bf.contacts)
)
SELECT infogroup_id,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Manager' THEN 1 ELSE 0 END) AS manager,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Other' THEN 1 ELSE 0 END) AS other,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Operations' THEN 1 ELSE 0 END) AS operations,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Sales' THEN 1 ELSE 0 END) AS sales,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Director' THEN 1 ELSE 0 END) AS director,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Vice President' THEN 1 ELSE 0 END) AS vice_president,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Site Manager' THEN 1 ELSE 0 END) AS site_manager,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Owner' THEN 1 ELSE 0 END) AS Owner,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Engineering/Technical' THEN 1 ELSE 0 END) AS engineering_technical,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Information Technology' THEN 1 ELSE 0 END) AS information_technology,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Finance' THEN 1 ELSE 0 END) AS finance,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Executive Officer' THEN 1 ELSE 0 END) AS executive_officer,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'General Manager' THEN 1 ELSE 0 END) AS general_manager,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Human Resources' THEN 1 ELSE 0 END) AS human_resources,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Regional Manager' THEN 1 ELSE 0 END) AS regional_manager,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Marketing' THEN 1 ELSE 0 END) AS marketing,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Sales Executive' THEN 1 ELSE 0 END) AS sales_executive,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Educator' THEN 1 ELSE 0 END) AS educator,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Board Member' THEN 1 ELSE 0 END) AS board_member,
       SUM(CASE WHEN split_part(t.labels_title_codes, ',', s.gen_num) = 'Senior Vice President' THEN 1 ELSE 0 END) AS senior_vice_president,
       SUM(CASE WHEN split_part(labels_title_codes, ',', s.gen_num) = 'Administrator' THEN 1 ELSE 0 END) AS administrator,
       SUM(CASE WHEN split_part(labels_title_codes, ',', s.gen_num) = 'President' THEN 1 ELSE 0 END) AS president
FROM core_bf.contacts AS t
JOIN splitter AS s ON 1 = 1
WHERE split_part(t.labels_title_codes, ',', s.gen_num) <> ''
GROUP BY t.infogroup_id
);

/*create table which takes sum of each 'labels_title_codes' along each infogroup_id by splitting and exploding 'ancestor_headquarters_ids' */
DROP TABLE IF EXISTS datascience.merge_places_contacts;
CREATE TABLE datascience.merge_places_contacts AS (
WITH NS AS (  SELECT 1 AS n UNION ALL
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
SELECT TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n)) AS infogroup_id,
       SUM(CASE WHEN manager IS NULL THEN 0 ELSE manager END) AS manager,
       SUM(CASE WHEN other IS NULL THEN 0 ELSE other END) AS other,
       SUM(CASE WHEN operations IS NULL THEN 0 ELSE operations END) AS operations,
       SUM(CASE WHEN sales IS NULL THEN 0 ELSE sales END) AS sales,
       SUM(CASE WHEN director IS NULL THEN 0 ELSE director END) AS director,
       SUM(CASE WHEN vice_president IS NULL THEN 0 ELSE vice_president END) AS vice_president,
       SUM(CASE WHEN site_manager IS NULL THEN 0 ELSE site_manager END) AS site_manager,
       SUM(CASE WHEN owner IS NULL THEN 0 ELSE owner END) AS owner,
       SUM(CASE WHEN engineering_technical IS NULL THEN 0 ELSE engineering_technical END) AS engineering_technical,
       SUM(CASE WHEN information_technology IS NULL THEN 0 ELSE information_technology END) AS information_technology,
       SUM(CASE WHEN finance IS NULL THEN 0 ELSE finance END) AS finance,
       SUM(CASE WHEN executive_officer IS NULL THEN 0 ELSE executive_officer END) AS executive_officer,
       SUM(CASE WHEN general_manager IS NULL THEN 0 ELSE general_manager END) AS general_manager,
       SUM(CASE WHEN human_resources IS NULL THEN 0 ELSE human_resources END) AS human_resources,
       SUM(CASE WHEN regional_manager IS NULL THEN 0 ELSE regional_manager END) AS regional_manager,
       SUM(CASE WHEN marketing IS NULL THEN 0 ELSE marketing END) AS marketing,
       SUM(CASE WHEN sales_executive IS NULL THEN 0 ELSE sales_executive END) AS sales_executive,
       SUM(CASE WHEN educator IS NULL THEN 0 ELSE educator END) AS educator,
       SUM(CASE WHEN board_member IS NULL THEN 0 ELSE board_member END) AS board_member,
       SUM(CASE WHEN senior_vice_president IS NULL THEN 0 ELSE senior_vice_president END) AS senior_vice_president,
       SUM(CASE WHEN administrator IS NULL THEN 0 ELSE administrator END) AS administrator,
       SUM(CASE WHEN president IS NULL THEN 0 ELSE president END) AS president
FROM NS
INNER JOIN
core_bf.places B
ON NS.n <= REGEXP_COUNT(B.ancestor_headquarters_ids, ',') + 1
LEFT JOIN datascience.merge_contacts a ON B.infogroup_id = a.infogroup_id
WHERE place_type IN ('branch', 'headquarters') AND in_business = 'yes'
AND TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n))<>''
GROUP BY TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n))

UNION ALL

SELECT a.infogroup_id,
       b.manager,
       b.other,
       b.operations,
       b.sales,
       b.director,
       b.vice_president,
       b.site_manager,
       b.owner,
       b.engineering_technical,
       b.information_technology,
       b.finance,
       b.executive_officer,
       b.general_manager,
       b.human_resources,
       b.regional_manager,
       b.marketing,
       b.sales_executive,
       b.Educator,
       b.Board_Member,
       b.senior_vice_president,
       b.administrator,
       b.president
FROM core_bf.places a
LEFT JOIN datascience.merge_contacts b 
ON a.infogroup_id = b.infogroup_id
WHERE place_type IN ('branch', 'headquarters') AND in_business = 'yes'
AND ancestor_headquarters_ids=''
);

/* create final_merge_places_contacts table */
DROP TABLE IF EXISTS datascience.final_merge_places_contacts;
CREATE TABLE datascience.final_merge_places_contacts AS (
SELECT infogroup_id,
       SUM(manager) AS manager,
       SUM(other) AS other,
       SUM(operations) AS operations,
       SUM(sales) AS sales,
       SUM(director) AS director,
       SUM(vice_president) AS vice_president,
       SUM(site_manager) AS site_manager,
       SUM(owner) AS owner,
       SUM(engineering_technical) AS engineering_technical,
       SUM(information_technology) AS information_technology,
       SUM(finance) AS finance,
       SUM(executive_officer) AS executive_officer,
       SUM(general_manager) AS general_manager,
       SUM(human_resources) AS human_resources,
       SUM(regional_manager) AS regional_manager,
       SUM(marketing) AS marketing,
       SUM(sales_executive) AS sales_executive,
       SUM(educator) AS educator,
       SUM(board_member) AS board_member,
       SUM(senior_vice_president) AS senior_vice_president,
       SUM(administrator) AS administrator,
       SUM(president) AS president
FROM datascience.merge_places_contacts
GROUP BY infogroup_id
);

/* created feature feature_eng_processing table which takes branch_location_employee_count,branch_location_sales_revenue
new_branch_count,location_intents_count along each splitted ancestor_headquarters_ids*/
DROP TABLE IF EXISTS datascience.feature_eng_processing;
CREATE TABLE datascience.feature_eng_processing AS (
WITH NS AS (
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
      TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n)) AS infogroup_id,
      SUM(location_employee_count) AS branch_location_employee_count,
      SUM(location_sales_volume) AS branch_location_sales_revenue,
      COUNT(infogroup_id) AS new_branch_count,
      SUM(location_intents_count) AS location_intents_count_fe,
      SUM(estimated_location_employee_count) AS branch_estimated_location_employee_count
FROM NS
INNER JOIN 
    core_bf.places B 
ON NS.n <= REGEXP_COUNT(B.ancestor_headquarters_ids, ',') + 1
WHERE in_business = 'yes' 
AND  TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n))<>''
GROUP BY TRIM(SPLIT_PART(B.ancestor_headquarters_ids, ',', NS.n))
);

/* create temp_corporate_sales_volume table which join feature_engg with final_merge_contact_places table 
and imputing data for some columns*/
DROP TABLE IF EXISTS datascience.temp_corporate_sales_volume;
CREATE TABLE datascience.temp_corporate_sales_volume AS (
SELECT COALESCE(b.manager,-1) AS manager,
       COALESCE(b.other,-1) AS other,
       COALESCE(b.operations,-1) AS operations,
       COALESCE(b.sales,-1) AS sales,
       COALESCE(b.director,-1) AS director,
       COALESCE(b.vice_president,-1) AS vice_president,
       COALESCE(b.site_manager,-1) AS site_manager,
       COALESCE(b.owner,-1) AS owner,
       COALESCE(b.engineering_technical,-1) AS engineering_technical,
       COALESCE(b.information_technology,-1) AS information_technology,
       COALESCE(b.finance,-1) AS finance,
       COALESCE(b.executive_officer,-1) AS executive_officer,
       COALESCE(b.general_manager,-1) AS general_manager,
       COALESCE(b.human_resources,-1) AS human_resources,
       COALESCE(b.regional_manager,-1) AS regional_manager,
       COALESCE(b.marketing,-1) AS marketing,
       COALESCE(b.sales_executive,-1) AS sales_executive,
       COALESCE(b.Educator,-1) AS Educator,
       COALESCE(b.Board_Member,-1) AS Board_Member,
       COALESCE(b.senior_vice_president,-1) AS senior_vice_president,
       COALESCE(b.administrator,-1) AS administrator,
       COALESCE(b.president,-1) AS president,       
       COALESCE(CASE WHEN c.branch_location_employee_count IS NULL THEN a.location_employee_count ELSE a.location_employee_count+c.branch_location_employee_count END,
        median(CASE WHEN c.branch_location_employee_count IS NULL THEN a.location_employee_count ELSE a.location_employee_count+c.branch_location_employee_count END) OVER ()) AS branch_location_employee_count,
       c.branch_location_sales_revenue,
       COALESCE(c.new_branch_count,-1) AS new_branch_count,
       COALESCE(c.location_intents_count_fe,median(c.location_intents_count_fe) OVER ()) AS location_intents_count_fe,
       COALESCE(CASE WHEN c.branch_estimated_location_employee_count IS NULL THEN a.location_employee_count ELSE a.location_employee_count+c.branch_estimated_location_employee_count END,
        median(CASE WHEN c.branch_estimated_location_employee_count IS NULL THEN a.location_employee_count ELSE a.location_employee_count+c.branch_estimated_location_employee_count END) OVER ()) AS branch_estimated_location_employee_count,
       a.infogroup_id,
       a.headquarters_id,
       a.in_business,
       CASE WHEN a.verified_on='' THEN a.verified_on ELSE CAST(DATEDIFF(day,TO_DATE(a.verified_on, 'YYYY-MM-DD'),current_date) as varchar(20)) END AS verification_recency,  
       CASE WHEN a.in_business_on='' THEN a.in_business_on ELSE CAST(DATEDIFF(day,TO_DATE(a.in_business_on, 'YYYY-MM-DD'),current_date) as varchar(20)) END AS in_business_since,
       a.city, 
       a.state,
       a.postal_code, 
       a.country_code, 
       a.primary_sic_code_id, 
       a.sic_code_ids,
       a.sic_code_ids_count, 
       a.primary_naics_code_id, 
       left(a.primary_naics_code_id,2) AS business_type,
       a.naics_code_ids,
       a.naics_code_ids_count,
       a.corporate_employee_count, 
       a.corporate_sales_revenue, 
       a.branch_count,
       COALESCE(a.eins_count,-1) AS eins_count, 
       a.has_ecommerce, 
       a.cbsa_code, 
       a.cbsa_level, 
       a.csa_code,
       a.census_block, 
       COALESCE(a.ucc_filings_count,-1) AS ucc_filings_count, 
       a.population_density,
       a.population_code_for_zip, 
       COALESCE(a.contacts_count,median(a.contacts_count) OVER ()) AS contacts_count,
       a.place_type,
       COALESCE(a.location_employee_count,median(a.location_employee_count) OVER ()) AS location_employee_count,
       a.primary_contact_job_titles_count AS "primary_contact.job_titles_count",
       CASE WHEN  a.facebook_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.twitter_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.linked_in_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.yelp_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.pinterest_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.youtube_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.foursquare_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.instagram_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.logo_url='' THEN 0 ELSE 1 END +
       CASE WHEN  a.tumblr_url='' THEN 0 ELSE 1 END AS socials_score,
       CASE WHEN  a.headquarters_id='' THEN 1 ELSE 0 END AS hq_type
FROM core_bf.places  a
LEFT JOIN datascience.final_merge_places_contacts b 
ON a.infogroup_id=b.infogroup_id
LEFT JOIN datascience.feature_eng_processing c
ON b.infogroup_id=c.infogroup_id
WHERE a.place_type = 'headquarters' AND a.in_business = 'yes'
);

/* create new variable subsidairy_count that contains no. of subsidiary each hq has and create final
corporate_sales_volume */
DROP TABLE IF EXISTS datascience.subsidairy_count;
CREATE TABLE datascience.subsidairy_count AS (
    SELECT  headquarters_id AS infogroup_id ,count(*) AS count 
    FROM datascience.temp_corporate_sales_volume
    GROUP BY headquarters_id
); 


DROP TABLE IF EXISTS datascience.corporate_sales_volume;
CREATE TABLE datascience.corporate_sales_volume AS (
  SELECT a.*,
         CASE WHEN b.count IS NOT NULL THEN b.count ELSE 0 END AS subsidairy_count 
  FROM datascience.temp_corporate_sales_volume a
  LEFT JOIN datascience.subsidairy_count b
  ON a.infogroup_id=b.infogroup_id
);



/*dump final table to s3 */
UNLOAD ('SELECT * FROM datascience.corporate_sales_volume')
TO 's3://{s3-datascience}/{ds_corporate_sales_s3_key}/corporate_sales_volume_' 
IAM_ROLE '{iam}'
DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
GZIP
; 