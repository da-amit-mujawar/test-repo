DROP TABLE if exists reports.tableau_raw_data;
---Added the dimensions/filters in the beginning
SELECT place_type,
CASE
        WHEN (primary_sic_code_id = '') THEN ('false')
        ELSE ('true')
       END primary_sic_flg,
       primary_sic_code_id,
       sic_code_ids,
       country_code,
       CASE
         WHEN country_code = 'US' THEN substring(postal_code,1,5)
         WHEN country_code = 'CA' THEN substring(postal_code,1,6)
       END postal_code,
       state,
       city,
      -- Use the primary_contact_primary 
       --CASE
        -- WHEN primary_contact_id = '' THEN ('false')
         --ELSE ('true')
       --END primary_contact,
       CASE
         WHEN primary_contact_primary = '' THEN ('false')
           ELSE ('true')
         END primary_contact,
        
       
       pt.labels_professional_title primary_contact_professional_title,
       primary_contact_title_codes,
       primary_contact_management_level,
       CASE
         WHEN (primary_contact_email = '') THEN ('false')
         ELSE ('true')
       END primary_contact_email,
       primary_contact_email_deliverable,
       primary_contact_email_marketable,
       CASE
         WHEN (corporate_sales_revenue < 50000) THEN ('$0-$499,999')
         WHEN (corporate_sales_revenue >= 50000 AND corporate_sales_revenue <= 999999) THEN ('$500,000-$999,999')
         WHEN (corporate_sales_revenue >= 1000000 AND corporate_sales_revenue <= 2499999) THEN ('$1,000,000-$2,499,999')
         WHEN (corporate_sales_revenue >= 2500000 AND corporate_sales_revenue <= 4999999) THEN ('$2,500,000-$4,999,999')
         WHEN (corporate_sales_revenue >= 5000000 AND corporate_sales_revenue <= 9999999) THEN ('$5,000,000-$9,999,999')
         WHEN (corporate_sales_revenue >= 10000000 AND corporate_sales_revenue <= 19999999) THEN ('$10,000,000-$19,999,999')
         WHEN (corporate_sales_revenue >= 20000000 AND corporate_sales_revenue <= 49999999) THEN ('$20,000,000-$49,999,999')
         WHEN (corporate_sales_revenue >= 50000000 AND corporate_sales_revenue <= 99999999) THEN ('$50,000,000-$99,999,999')
         WHEN (corporate_sales_revenue >= 100000000 AND corporate_sales_revenue <= 499999999) THEN ('$100,000,000-$499,999,999')
         WHEN (corporate_sales_revenue >= 500000000 AND corporate_sales_revenue <= 999999999) THEN ('$500,000,000-$999,999,999')
         WHEN (corporate_sales_revenue >= 1000000000) THEN ('$1,000,000,000+')
       END corporate_sales_volume,
       CASE
         WHEN (location_sales_volume < 50000) THEN ('$0-$499,999')
         WHEN (location_sales_volume >= 50000 AND location_sales_volume <= 999999) THEN ('$500,$000-999,999')
         WHEN (location_sales_volume >= 1000000 AND location_sales_volume <= 2499999) THEN ('$1,000,000-$2,499,999')
         WHEN (location_sales_volume >= 2500000 AND location_sales_volume <= 4999999) THEN ('$2,500,000-$4,999,999')
         WHEN (location_sales_volume >= 5000000 AND location_sales_volume <= 9999999) THEN ('$5,000,000-$9,999,999')
         WHEN (location_sales_volume >= 10000000 AND location_sales_volume <= 19999999) THEN ('$10,000,000-$19,999,999')
         WHEN (location_sales_volume >= 20000000 AND location_sales_volume <= 49999999) THEN ('$20,000,000-$49,999,999')
         WHEN (location_sales_volume >= 50000000 AND location_sales_volume <= 99999999) THEN ('$50,000,000-$99,999,999')
         WHEN (location_sales_volume >= 100000000 AND location_sales_volume <= 499999999) THEN ('$100,000,000-$499,999,999')
         WHEN (location_sales_volume >= 500000000 AND location_sales_volume <= 999999999) THEN ('$500,000,000-$999,999,999')
         WHEN (location_sales_volume >= 1000000000) THEN ('$1,000,000,000+')
       END location_sales_volume,
       CASE
         WHEN (estimated_corporate_sales_revenue < 50000) THEN ('$0-$499,999')
         WHEN (estimated_corporate_sales_revenue >= 50000 AND estimated_corporate_sales_revenue <= 999999) THEN ('$500,$000-999,999')
         WHEN (estimated_corporate_sales_revenue >= 1000000 AND estimated_corporate_sales_revenue <= 2499999) THEN ('$1,000,000-$2,499,999')
         WHEN (estimated_corporate_sales_revenue >= 2500000 AND estimated_corporate_sales_revenue <= 4999999) THEN ('$2,500,000-$4,999,999')
         WHEN (estimated_corporate_sales_revenue >= 5000000 AND estimated_corporate_sales_revenue <= 9999999) THEN ('$5,000,000-$9,999,999')
         WHEN (estimated_corporate_sales_revenue >= 10000000 AND estimated_corporate_sales_revenue <= 19999999) THEN ('$10,000,000-$19,999,999')
         WHEN (estimated_corporate_sales_revenue >= 20000000 AND estimated_corporate_sales_revenue <= 49999999) THEN ('$20,000,000-$49,999,999')
         WHEN (estimated_corporate_sales_revenue >= 50000000 AND estimated_corporate_sales_revenue <= 99999999) THEN ('$50,000,000-$99,999,999')
         WHEN (estimated_corporate_sales_revenue >= 100000000 AND estimated_corporate_sales_revenue <= 499999999) THEN ('$100,000,000-$499,999,999')
         WHEN (estimated_corporate_sales_revenue >= 500000000 AND estimated_corporate_sales_revenue <= 999999999) THEN ('$500,000,000-$999,999,999')
         WHEN (estimated_corporate_sales_revenue >= 1000000000) THEN ('$1,000,000,000+')
       END estimated_corporate_sales_revenue,
       CASE
         WHEN (location_employee_count >= 1 AND location_employee_count <= 4) THEN ('1-4')
         WHEN (location_employee_count >= 5 AND location_employee_count <= 9) THEN ('5-9')
         WHEN (location_employee_count >= 10 AND location_employee_count <= 19) THEN ('10-19')
         WHEN (location_employee_count >= 20 AND location_employee_count <= 49) THEN ('20-49')
         WHEN (location_employee_count >= 50 AND location_employee_count <= 99) THEN ('50-99')
         WHEN (location_employee_count >= 100 AND location_employee_count <= 249) THEN ('100-249')
         WHEN (location_employee_count >= 200 AND location_employee_count <= 499) THEN ('250-499')
         WHEN (location_employee_count >= 500 AND location_employee_count <= 999) THEN ('500-999')
         WHEN (location_employee_count >= 1000 AND location_employee_count <= 4999) THEN ('1,000-4,999')
         WHEN (location_employee_count >= 5000 AND location_employee_count <= 9999) THEN ('5,000-9,999')
         WHEN (location_employee_count >= 10000) THEN ('10,000+')
       END location_employee_size,
       CASE
         WHEN (corporate_employee_count >= 1 AND corporate_employee_count <= 4) THEN ('1-4')
         WHEN (corporate_employee_count >= 5 AND corporate_employee_count <= 9) THEN ('5-9')
         WHEN (corporate_employee_count >= 10 AND corporate_employee_count <= 19) THEN ('10-19')
         WHEN (corporate_employee_count >= 20 AND corporate_employee_count <= 49) THEN ('20-49')
         WHEN (corporate_employee_count >= 50 AND corporate_employee_count <= 99) THEN ('50-99')
         WHEN (corporate_employee_count >= 100 AND corporate_employee_count <= 249) THEN ('100-249')
         WHEN (corporate_employee_count >= 200 AND corporate_employee_count <= 499) THEN ('250-499')
         WHEN (corporate_employee_count >= 500 AND corporate_employee_count <= 999) THEN ('500-999')
         WHEN (corporate_employee_count >= 1000 AND corporate_employee_count <= 4999) THEN ('1,000-4,999')
         WHEN (corporate_employee_count >= 5000 AND corporate_employee_count <= 9999) THEN ('5,000-9,999')
         WHEN (corporate_employee_count >= 10000) THEN ('10,000+')
       END corporate_employee_size,
       CASE
         WHEN (estimated_location_employee_count >= 1 AND estimated_location_employee_count <= 4) THEN ('1-4')
         WHEN (estimated_location_employee_count >= 5 AND estimated_location_employee_count <= 9) THEN ('5-9')
         WHEN (estimated_location_employee_count >= 10 AND estimated_location_employee_count <= 19) THEN ('10-19')
         WHEN (estimated_location_employee_count >= 20 AND estimated_location_employee_count <= 49) THEN ('20-49')
         WHEN (estimated_location_employee_count >= 50 AND estimated_location_employee_count <= 99) THEN ('50-99')
         WHEN (estimated_location_employee_count >= 100 AND estimated_location_employee_count <= 249) THEN ('100-249')
         WHEN (estimated_location_employee_count >= 200 AND estimated_location_employee_count <= 499) THEN ('250-499')
         WHEN (estimated_location_employee_count >= 500 AND estimated_location_employee_count <= 999) THEN ('500-999')
         WHEN (estimated_location_employee_count >= 1000 AND estimated_location_employee_count <= 4999) THEN ('1,000-4,999')
         WHEN (estimated_location_employee_count >= 5000 AND estimated_location_employee_count <= 9999) THEN ('5,000-9,999')
         WHEN (estimated_location_employee_count >= 10000) THEN ('10,000+')
       END estimated_location_employee_size,
       CASE
         WHEN (estimated_corporate_employee_count >= 1 AND estimated_corporate_employee_count <= 4) THEN ('1-4')
         WHEN (estimated_corporate_employee_count >= 5 AND estimated_corporate_employee_count <= 9) THEN ('5-9')
         WHEN (estimated_corporate_employee_count >= 10 AND estimated_corporate_employee_count <= 19) THEN ('10-19')
         WHEN (estimated_corporate_employee_count >= 20 AND estimated_corporate_employee_count <= 49) THEN ('20-49')
         WHEN (estimated_corporate_employee_count >= 50 AND estimated_corporate_employee_count <= 99) THEN ('50-99')
         WHEN (estimated_corporate_employee_count >= 100 AND estimated_corporate_employee_count <= 249) THEN ('100-249')
         WHEN (estimated_corporate_employee_count >= 200 AND estimated_corporate_employee_count <= 499) THEN ('250-499')
         WHEN (estimated_corporate_employee_count >= 500 AND estimated_corporate_employee_count <= 999) THEN ('500-999')
         WHEN (estimated_corporate_employee_count >= 1000 AND estimated_corporate_employee_count <= 4999) THEN ('1,000-4,999')
         WHEN (estimated_corporate_employee_count >= 5000 AND estimated_corporate_employee_count <= 9999) THEN ('5,000-9,999')
         WHEN (estimated_corporate_employee_count >= 10000) THEN ('10,000+')
       END Estimated_corporate_employee_size,
       CASE
         WHEN (website = '') THEN ('false')
         ELSE ('true')
       END website_ind,
       CASE
         WHEN (facebook_url = '') THEN ('false')
         ELSE ('true')
       END facebook_url_ind,
       CASE
         WHEN (twitter_url = '') THEN ('false')
         ELSE ('true')
       END twitter_url_ind,
       CASE
         WHEN (linked_in_url = '') THEN ('false')
         ELSE ('true')
       END linked_in_url_ind,
       CASE
         WHEN (yelp_url = '') THEN ('false')
         ELSE ('true')
       END yelp_url_ind,
       CASE
         WHEN (pinterest_url = '') THEN ('false')
         ELSE ('true')
       END pinterest_url_ind,
       CASE
         WHEN (youtube_url = '') THEN ('false')
         ELSE ('true')
       END youtube_url_ind,
       CASE
         WHEN (tumblr_url = '') THEN ('false')
         ELSE ('true')
       END tumblr_url_ind,
       CASE
         WHEN (foursquare_url = '') THEN ('false')
         ELSE ('true')
       END foursquare_url_ind,
       CASE
         WHEN (instagram_url = '') THEN ('false')
         ELSE ('true')
       END instagram_url_ind,
       CASE
         WHEN (logo_url = '') THEN ('false')
         ELSE ('true')
       END logo_url_ind,
       b.infogroup_id,

       --Transformation of elements for Phase 1. 
       
---Broad groups-core information, Categories-Names 
       CASE
         WHEN name = '' THEN NULL
         ELSE 1
       END infogroup_id_name,
 ---Broad groups-core information, Categories- Addresses 
       CASE
         WHEN street = '' THEN NULL
         ELSE 1
       END infogroup_id_street,
       CASE
         WHEN suite = '' THEN NULL
         ELSE 1
       END infogroup_id_suite,
       CASE
         WHEN city = '' THEN NULL
         ELSE 1
       END infogroup_id_city,
       CASE
         WHEN state = '' THEN NULL
         ELSE 1
       END infogroup_id_state,
       CASE
         WHEN postal_code = '' THEN NULL
         ELSE 1
       END infogroup_id_postal_code,
       
 ---Broad groups-core information, Categories- Categories     
 ---Need to add tags. 
         CASE
         WHEN primary_sic_code_id = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_sic_code_id,
       CASE
         WHEN sic_code_ids = '' THEN NULL
         ELSE 1
       END infogroup_id_sic_code_ids,
        CASE
         WHEN primary_naics_code_id = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_naics_code_id,
       CASE
         WHEN naics_code_ids = '' THEN NULL
         ELSE 1
       END infogroup_id_naics_code_ids,
       
---Broad groups-core information, Categories- Contact Info 
       CASE
         WHEN phone = '' THEN NULL
         ELSE 1
       END infogroup_id_phone, 
       ---Broad groups-core information, Categories- Websites      
       
       CASE
         WHEN website = '' THEN NULL
         ELSE 1
       END infogroup_id_website,
       --Need to add website status not in places table. 
       --CASE
        -- WHEN website_status = '' THEN NULL
        -- ELSE b.infogroup_id
      -- END infogroup_id_website_status, 
CASE
         WHEN facebook_url = '' THEN NULL
         ELSE 1
       END infogroup_id_facebook_url,
       CASE
         WHEN twitter_url = '' THEN NULL
         ELSE 1
       END infogroup_id_twitter_url,
       CASE
         WHEN linked_in_url = '' THEN NULL
         ELSE 1
       END infogroup_id_linked_in_url,
       CASE
         WHEN yelp_url = '' THEN NULL
         ELSE 1
       END infogroup_id_yelp_url,
       CASE
         WHEN pinterest_url = '' THEN NULL
         ELSE 1
       END infogroup_id_pinterest_url,
       CASE
         WHEN youtube_url = '' THEN NULL
         ELSE 1 
       END infogroup_id_youtube_url,
       CASE
         WHEN tumblr_url = '' THEN NULL
         ELSE 1
       END infogroup_id_tumblr_url,
       CASE
         WHEN foursquare_url = '' THEN NULL
         ELSE 1
       END infogroup_id_foursquare_url,
       CASE
         WHEN instagram_url = '' THEN NULL
         ELSE 1
       END infogroup_id_instagram_url,
       CASE
         WHEN logo_url = '' THEN NULL
         ELSE 1
       END infogroup_id_logo_url,  
       ---Broad groups-core information, Categories- Employees   
      CASE
         WHEN primary_contact_id = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_id,  
      CASE
         WHEN primary_contact_email = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_email,
       CASE
          WHEN primary_contact_email_md5 = '' THEN NULL
          ELSE 1
       END infogroup_id_primary_contact_email_md5,       
       CASE
         WHEN primary_contact_email_sha256 = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_email_sha256, 
       CASE
         WHEN primary_contact_email_vendor_id = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_email_vendor_id,      
       CASE
         WHEN primary_contact_email_deliverable = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_email_deliverable,      
       CASE
         WHEN primary_contact_email_marketable = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_email_marketable,     
       CASE
         WHEN primary_contact_email_reputation_risk = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_email_reputation_risk, 
       CASE
         WHEN primary_contact_first_name = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_first_name,  
       CASE
          WHEN primary_contact_gender = '' THEN NULL
          ELSE 1
        END infogroup_id_primary_contact_gender, 
        CASE
           WHEN primary_contact_job_function_id IS NOT NULL THEN 1
       END infogroup_id_primary_contact_job_function_id,
       CASE
         WHEN primary_contact_job_titles = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_job_titles,    
       CASE
         WHEN primary_contact_job_title_ids = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_job_title_ids, 
       CASE
         WHEN primary_contact_last_name = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_last_name,  
       CASE
         WHEN primary_contact_management_level = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_management_level, 
       CASE
         WHEN primary_contact_mapped_contact_id = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_mapped_contact_id, 
       CASE
         WHEN primary_contact_professional_title = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_professional_title, 
       CASE
         WHEN primary_contact_primary = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_primary, 
       CASE
         WHEN primary_contact_title_codes = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_title_codes,   
       CASE
         WHEN primary_contact_vendor_id = '' THEN NULL
         ELSE 1
       END infogroup_id_primary_contact_vendor_id,
       CASE
         WHEN (location_employee_count >= 1) THEN 1
         ELSE 0
        END infogroup_id_location_employee_count,
       CASE
         WHEN (estimated_location_employee_count >= 1) THEN 1
         ELSE 0
       END infogroup_id_estimated_location_employee_count,
       CASE
         WHEN (corporate_employee_count >= 1 ) THEN 1
         ELSE 0
       END infogroup_id_corporate_employee_count,
       CASE
         WHEN (estimated_corporate_employee_count  >=1)  THEN 1
         ELSE 0
       END infogroup_id_estimated_corporate_employee_count,
       CASE
         WHEN corporate_sales_revenue IS NOT NULL THEN 1
         ELSE 0
       END infogroup_id_corporate_sales_revenue,
       CASE
         WHEN estimated_corporate_sales_revenue IS NOT NULL THEN 1
         ELSE 0
       END infogroup_id_estimated_corporate_sales_revenue,
       CASE
         WHEN location_sales_volume  IS NOT NULL THEN 1
         ELSE 0
       END infogroup_id_location_sales_volume,
case when (place_type='headquarters') then (1) else (0)
end infogroup_id_headquarters_id,
infogroup_id_adsize_code_tags,
infogroup_id_primary_tags,
infogroup_id_sic_code_id_tags,
infogroup_id_naics_code_id_tags,
infogroup_id_yellow_page_code_tags,
infogroup_id_yppa_code_tags
       INTO reports.tableau_raw_data
FROM core_bf.mv_places_in_business b
left join reports.tableau_tags_data_final t
on (t.infogroup_id=b.infogroup_id)
left join reports.professional_title_lk pt
on (b.primary_contact_professional_title=pt.professional_title);

--select count(infogroup_id_location_sales_volume) from reports.tableau_raw_data ;
-- 16861345