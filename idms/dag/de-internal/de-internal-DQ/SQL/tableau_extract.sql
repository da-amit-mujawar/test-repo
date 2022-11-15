DROP TABLE if exists workspace.tableau_extract;

SELECT place_type,
primary_sic_flg,
       primary_sic_code_id,
       sic_code_ids,
       country_code,
       postal_code,
       state,
       city,
       primary_contact,
       primary_contact_professional_title,
       primary_contact_title_codes,
       primary_contact_management_level,
       primary_contact_email,
       primary_contact_email_deliverable,
       primary_contact_email_marketable,
       corporate_sales_volume,
       location_sales_volume,
       estimated_corporate_sales_revenue,
       location_employee_size,
       corporate_employee_size,
       estimated_location_employee_size,
       estimated_corporate_employee_size,
       website_ind,
       facebook_url_ind,
       twitter_url_ind,
       linked_in_url_ind,
       yelp_url_ind,
       pinterest_url_ind,
       youtube_url_ind,
       tumblr_url_ind,
       foursquare_url_ind,
       instagram_url_ind,
       logo_url_ind,
       COUNT(infogroup_id) cnt,
       SUM(infogroup_id_name) cnt_name,
       SUM(infogroup_id_street) cnt_street,
       SUM(infogroup_id_suite) cnt_suite,
       SUM(infogroup_id_city) cnt_city,
       SUM(infogroup_id_state) cnt_state,
       SUM(infogroup_id_postal_code) cnt_postal_code,
       SUM(infogroup_id_primary_sic_code_id) cnt_primary_sic_code_id,
       SUM(infogroup_id_sic_code_ids)cnt_sic_code_ids,
       SUM(infogroup_id_primary_naics_code_id) cnt_primary_naics_code_id,
       SUM(infogroup_id_naics_code_ids) cnt_naics_code_ids,
       SUM(infogroup_id_phone) cnt_phone,
       SUM(infogroup_id_website) cnt_website,
       --SUM(infogroup_id_website_status) cnt_website_status,
       SUM(infogroup_id_facebook_url) cnt_facebook_url,
       SUM(infogroup_id_twitter_url) cnt_twitter_url,
       SUM(infogroup_id_linked_in_url) cnt_linked_in_url,
       SUM(infogroup_id_yelp_url) cnt_yelp_url,
       SUM(infogroup_id_pinterest_url) cnt_pinterest_url,
       SUM(infogroup_id_youtube_url) cnt_youtube_url,
       SUM(infogroup_id_tumblr_url) cnt_tumblr_url,
       SUM(infogroup_id_foursquare_url) cnt_foursquare_url,
       SUM(infogroup_id_instagram_url) cnt_instagram_url,
       SUM(infogroup_id_logo_url) cnt_logo_url,
       --SUM(infogroup_id_bbb_business_review_url) cnt_bbb_business_review_url,
       --SUM(infogroup_id_single_platform_url) cnt_single_platform_url,
       SUM(infogroup_id_primary_contact_id) cnt_primary_contact_id,
       SUM(infogroup_id_primary_contact_email) cnt_primary_contact_email,
       SUM(infogroup_id_primary_contact_email_md5) cnt_primary_contact_email_md5,
       SUM(infogroup_id_primary_contact_email_sha256) cnt_primary_contact_email_sha256,
       SUM(infogroup_id_primary_contact_email_vendor_id) cnt_primary_contact_email_vendor_id,
       SUM(infogroup_id_primary_contact_email_deliverable) cnt_primary_contact_email_deliverable,
       SUM(infogroup_id_primary_contact_email_marketable) cnt_primary_contact_email_marketable,
       SUM(infogroup_id_primary_contact_email_reputation_risk) cnt_primary_contact_email_reputation_risk,
       SUM(infogroup_id_primary_contact_first_name) cnt_primary_contact_first_name,
       SUM(infogroup_id_primary_contact_gender) cnt_primary_contact_gender,
       SUM(infogroup_id_primary_contact_job_function_id) cnt_primary_contact_job_function_id,
       SUM(infogroup_id_primary_contact_job_titles) cnt_primary_contact_job_titles,
       SUM(infogroup_id_primary_contact_job_title_ids) cnt_primary_contact_job_title_ids,
       SUM(infogroup_id_primary_contact_last_name) cnt_primary_contact_last_name,
       SUM(infogroup_id_primary_contact_management_level) cnt_primary_contact_management_level,
       SUM(infogroup_id_primary_contact_mapped_contact_id) cnt_primary_contact_mapped_contact_id,
       SUM(infogroup_id_primary_contact_professional_title) cnt_primary_contact_professional_title,
       SUM(infogroup_id_primary_contact_primary) cnt_primary_contact_primary,
       SUM(infogroup_id_primary_contact_title_codes) cnt_primary_contact_title_codes,
       SUM(infogroup_id_primary_contact_vendor_id) cnt_primary_contact_vendor_id,
       SUM(infogroup_id_location_employee_count) cnt_location_employee_count,
       SUM(infogroup_id_estimated_location_employee_count) cnt_estimated_location_employee_count,
       SUM(infogroup_id_corporate_employee_count) cnt_corporate_employee_count,
       SUM(infogroup_id_estimated_corporate_employee_count) cnt_estimated_corporate_employee_count,
       SUM(infogroup_id_corporate_sales_revenue) cnt_corporate_sales_revenue,
       SUM(infogroup_id_estimated_corporate_sales_revenue) cnt_estimated_corporate_sales_revenue,
       SUM(infogroup_id_location_sales_volume) cnt_location_sales_volume,
       sum(infogroup_id_headquarters_id) cnt_headquarters_id,
       SUM(infogroup_id_adsize_code_tags) cnt_adsize_code_tags,
       SUM(infogroup_id_primary_tags) cnt_primary_tags,
       SUM(infogroup_id_sic_code_id_tags) cnt_sic_code_id_tags,
       SUM(infogroup_id_naics_code_id_tags) cnt_naics_code_id_tags,
       SUM(infogroup_id_yellow_page_code_tags) cnt_yellow_page_code_tags,
       SUM(infogroup_id_yppa_code_tags) cnt_id_yppa_code_tags
        INTO workspace.tableau_extract
  
FROM reports.tableau_raw_data
GROUP BY place_type,primary_sic_flg,
       primary_sic_code_id,
       sic_code_ids,
         country_code,
         postal_code,
         state,
         city,
         primary_contact,
         primary_contact_professional_title,
         primary_contact_title_codes,
         primary_contact_management_level,
         primary_contact_email,
         primary_contact_email_deliverable,
         primary_contact_email_marketable,
         corporate_sales_volume,
       location_sales_volume,
       estimated_corporate_sales_revenue,
       location_employee_size,
       corporate_employee_size,
       estimated_location_employee_size,
       estimated_corporate_employee_size,
       website_ind,
         facebook_url_ind,
         twitter_url_ind,
         linked_in_url_ind,
         yelp_url_ind,
         pinterest_url_ind,
         youtube_url_ind,
         tumblr_url_ind,
         foursquare_url_ind,
         instagram_url_ind,
         logo_url_ind
--          limit 10
         ;