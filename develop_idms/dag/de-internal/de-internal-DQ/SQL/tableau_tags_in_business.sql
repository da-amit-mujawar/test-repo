DROP TABLE if exists reports.tableau_tags_in_business;
-- create temp table with Tags data for In_business='y'
SELECT t.infogroup_id,
case when (t.adsize_code='') then (NULL) else (t.infogroup_id) end infogroup_id_adsize_code,
case when (t."primary"='true') then (t.infogroup_id) end infogroup_id_primary_tags,
case when (t.sic_code_id='') then (NULL) else (t.infogroup_id) end infogroup_id_sic_code_id,
case when (t.naics_code_id='') then (NULL) else (t.infogroup_id) end infogroup_id_naics_code_id,
case when (t.yellow_page_code='') then (NULL) else (t.infogroup_id) end infogroup_id_yellow_page_code,
case when (t.yppa_code='') then (NULL) else (t.infogroup_id) end infogroup_id_yppa_code INTO reports.tableau_tags_in_business
FROM core_bf.tags t
join core_bf.places b on b.infogroup_id=t.infogroup_id 
WHERE in_business = 'yes';

--summarized tags data at infogroup_id level
DROP TABLE reports.tableau_tags_data_final;
select infogroup_id,
count(distinct infogroup_id_adsize_code) infogroup_id_adsize_code_tags,
count(distinct infogroup_id_primary_tags) infogroup_id_primary_tags,
count(distinct infogroup_id_sic_code_id) infogroup_id_sic_code_id_tags,
count(distinct infogroup_id_naics_code_id) infogroup_id_naics_code_id_tags,
count(distinct infogroup_id_yellow_page_code) infogroup_id_yellow_page_code_tags,
count(distinct infogroup_id_yppa_code) infogroup_id_yppa_code_tags
 INTO reports.tableau_tags_data_final
from
reports.tableau_tags_in_business
group by infogroup_id;
