--Total-140 elements ; phase 1=54 measures (no website and prim cont id) , phase 2-76 elements + 3 extra elements on lat,
-- long & upper. removed in_business, --Phase4,5 6=7 elements.
--removed in_business & primary contact primary as a measures
--checked almost all elements
--the total should be 140 elements.
--Each phase is kept separately due to performance issues, especially when creating the tableau dashboards.
--Hence there will be 6 to 8 tables created in each phase.
--Places file is considered the master file and other files are treated as the child files.
DROP TABLE IF EXISTS reports.tableau_tags_in_business;
-- create temp table with Tags data for In_business='y'
SELECT t.infogroup_id,
       CASE WHEN (t.adsize_code = '') THEN (NULL) ELSE (t.infogroup_id) END      infogroup_id_adsize_code,
       CASE WHEN (t."primary" = 'true') THEN (t.infogroup_id) END                infogroup_id_primary_tags,
       CASE WHEN (t.sic_code_id = '') THEN (NULL) ELSE (t.infogroup_id) END      infogroup_id_sic_code_id,
       CASE WHEN (t.naics_code_id = '') THEN (NULL) ELSE (t.infogroup_id) END    infogroup_id_naics_code_id,
       CASE WHEN (t.yellow_page_code = '') THEN (NULL) ELSE (t.infogroup_id) END infogroup_id_yellow_page_code,
       CASE WHEN (t.yppa_code = '') THEN (NULL) ELSE (t.infogroup_id) END        infogroup_id_yppa_code
INTO reports.tableau_tags_in_business
FROM core_bf.tags t
         JOIN core_bf.places b ON b.infogroup_id = t.infogroup_id
WHERE in_business = 'yes';

--summarized tags data at infogroup_id level
DROP TABLE IF EXISTS reports.tableau_tags_data_final;
SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_adsize_code)      infogroup_id_adsize_code_tags,
       COUNT(DISTINCT infogroup_id_primary_tags)     infogroup_id_primary_tags,
       COUNT(DISTINCT infogroup_id_sic_code_id)      infogroup_id_sic_code_id_tags,
       COUNT(DISTINCT infogroup_id_naics_code_id)    infogroup_id_naics_code_id_tags,
       COUNT(DISTINCT infogroup_id_yellow_page_code) infogroup_id_yellow_page_code_tags,
       COUNT(DISTINCT infogroup_id_yppa_code)        infogroup_id_yppa_code_tags
INTO reports.tableau_tags_data_final
FROM reports.tableau_tags_in_business
GROUP BY infogroup_id;

--select count(adsize_code) from core_bf.tags;
--select count(infogroup_id_adsize_code) from reports.tableau_tags_in_business

/*Adding the operation hours info*/
DROP TABLE IF EXISTS reports.tableau_op_in_business;
SELECT o.infogroup_id,
       CASE WHEN (o.start_time = '') THEN (NULL) ELSE (o.infogroup_id) END infogroup_id_start_time,
       CASE WHEN (o.end_time = '') THEN (NULL) ELSE (o.infogroup_id) END   infogroup_id_end_time,
--MAX(case when (o.start_time='00:00:00') then (NULL) else (o.infogroup_id) end) infogroup_id_start_time,
--MAX(case when (o.end_time='00:00:00') then (NULL) else (o.infogroup_id) end) infogroup_id_end_time,
       o.infogroup_id                                                      infogroup_id_days
--case when (o.days=' ') then (NULL) else (o.infogroup_id) end infogroup_id_days
INTO reports.tableau_op_in_business
FROM core_bf.operating_hours o
         JOIN core_bf.places b ON b.infogroup_id = o.infogroup_id
WHERE in_business = 'yes';

DROP TABLE IF EXISTS reports.tableau_op_data_final;
SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_start_time) infogroup_id_op_start_time,
       COUNT(DISTINCT infogroup_id_end_time)   infogroup_id_op_end_time,
       COUNT(DISTINCT infogroup_id_days)       infogroup_id_op_days
INTO reports.tableau_op_data_final
FROM reports.tableau_op_in_business
GROUP BY infogroup_id;



DROP TABLE IF EXISTS reports.tableau_happy_in_business;
-- create temp table with Tags data for In_business='y'
SELECT h.infogroup_id,
       CASE WHEN (h.id = '') THEN (NULL) ELSE (h.infogroup_id) END               infogroup_id_happy_id,
       CASE WHEN (h.special_food = '') THEN (NULL) ELSE (h.infogroup_id) END     infogroup_id_happy_special_food,
       CASE WHEN (h.special_drink = '') THEN (NULL) ELSE (h.infogroup_id) END    infogroup_id_happy_special_drink,
       CASE WHEN (h.special_activity = '') THEN (NULL) ELSE (h.infogroup_id) END infogroup_id_happy_special_activity,
       CASE WHEN (h.special_other = '') THEN (NULL) ELSE (h.infogroup_id) END    infogroup_id_happy_special_other,
       CASE WHEN (h.start_time = '') THEN (NULL) ELSE (h.infogroup_id) END       infogroup_id_happy_start_time,
       CASE WHEN (h.end_time = '') THEN (NULL) ELSE (h.infogroup_id) END         infogroup_id_happy_end_time,
       CASE WHEN (h.days = '') THEN (NULL) ELSE (h.infogroup_id) END             infogroup_id_happy_days
INTO reports.tableau_happy_in_business
FROM core_bf.happy_hours h
         JOIN core_bf.places b ON b.infogroup_id = h.infogroup_id
WHERE in_business = 'yes';


DROP TABLE IF EXISTS reports.tableau_happy_data_final;
SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_happy_id)               infogroup_id_happy_hour_id,
       COUNT(DISTINCT infogroup_id_happy_special_food)     infogroup_id_happy_hour_special_food,
       COUNT(DISTINCT infogroup_id_happy_special_drink)    infogroup_id_happy_hour_special_drink,
       COUNT(DISTINCT infogroup_id_happy_special_activity) infogroup_id_happy_hour_special_activity,
       COUNT(DISTINCT infogroup_id_happy_special_other)    infogroup_id_happy_hour_special_other,
       COUNT(DISTINCT infogroup_id_happy_start_time)       infogroup_id_happy_hour_start_time,
       COUNT(DISTINCT infogroup_id_happy_end_time)         infogroup_id_happy_hour_end_time,
       COUNT(DISTINCT infogroup_id_happy_days)             infogroup_id_happy_hour_days
INTO reports.tableau_happy_data_final
FROM reports.tableau_happy_in_business
GROUP BY infogroup_id;

--select count(infogroup_id_happy_hour_id) from reports.tableau_happy_data_final

--select count(infogroup_id_happy_id) from reports.tableau_happy_in_business

--select count(*) from core_bf.places where in_business='yes'

--select infogroup_id_happy_hour_id, count(1) from reports.tableau_happy_data_final
--group by infogroup_id_happy_hour_id


--Filters are being set up

DROP TABLE IF EXISTS reports.tableau_raw_data_phase12;
---Added the dimensions/filters in the beginning
SELECT place_type,
       CASE
           WHEN (primary_sic_code_id = '') THEN ('false')
           ELSE ('true')
           END                      primary_sic_flg,
       primary_sic_code_id,
       sic_code_ids,
       country_code,
       CASE
           WHEN country_code = 'US' THEN SUBSTRING(postal_code, 1, 5)
           WHEN country_code = 'CA' THEN SUBSTRING(postal_code, 1, 6)
           END                      postal_code,
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
           END                      primary_contact,
       pt.labels_professional_title primary_contact_professional_title,
       primary_contact_title_codes,
       primary_contact_management_level,
       CASE
           WHEN (primary_contact_email = '') THEN ('false')
           ELSE ('true')
           END                      primary_contact_email,
       primary_contact_email_deliverable,
       primary_contact_email_marketable,
       CASE
           WHEN (corporate_sales_revenue < 50000) THEN ('$0-$499,999')
           WHEN (corporate_sales_revenue >= 50000 AND corporate_sales_revenue <= 999999) THEN ('$500,000-$999,999')
           WHEN (corporate_sales_revenue >= 1000000 AND corporate_sales_revenue <= 2499999)
               THEN ('$1,000,000-$2,499,999')
           WHEN (corporate_sales_revenue >= 2500000 AND corporate_sales_revenue <= 4999999)
               THEN ('$2,500,000-$4,999,999')
           WHEN (corporate_sales_revenue >= 5000000 AND corporate_sales_revenue <= 9999999)
               THEN ('$5,000,000-$9,999,999')
           WHEN (corporate_sales_revenue >= 10000000 AND corporate_sales_revenue <= 19999999)
               THEN ('$10,000,000-$19,999,999')
           WHEN (corporate_sales_revenue >= 20000000 AND corporate_sales_revenue <= 49999999)
               THEN ('$20,000,000-$49,999,999')
           WHEN (corporate_sales_revenue >= 50000000 AND corporate_sales_revenue <= 99999999)
               THEN ('$50,000,000-$99,999,999')
           WHEN (corporate_sales_revenue >= 100000000 AND corporate_sales_revenue <= 499999999)
               THEN ('$100,000,000-$499,999,999')
           WHEN (corporate_sales_revenue >= 500000000 AND corporate_sales_revenue <= 999999999)
               THEN ('$500,000,000-$999,999,999')
           WHEN (corporate_sales_revenue >= 1000000000) THEN ('$1,000,000,000+')
           END                      corporate_sales_volume,
       CASE
           WHEN (location_sales_volume < 50000) THEN ('$0-$499,999')
           WHEN (location_sales_volume >= 50000 AND location_sales_volume <= 999999) THEN ('$500,$000-999,999')
           WHEN (location_sales_volume >= 1000000 AND location_sales_volume <= 2499999) THEN ('$1,000,000-$2,499,999')
           WHEN (location_sales_volume >= 2500000 AND location_sales_volume <= 4999999) THEN ('$2,500,000-$4,999,999')
           WHEN (location_sales_volume >= 5000000 AND location_sales_volume <= 9999999) THEN ('$5,000,000-$9,999,999')
           WHEN (location_sales_volume >= 10000000 AND location_sales_volume <= 19999999)
               THEN ('$10,000,000-$19,999,999')
           WHEN (location_sales_volume >= 20000000 AND location_sales_volume <= 49999999)
               THEN ('$20,000,000-$49,999,999')
           WHEN (location_sales_volume >= 50000000 AND location_sales_volume <= 99999999)
               THEN ('$50,000,000-$99,999,999')
           WHEN (location_sales_volume >= 100000000 AND location_sales_volume <= 499999999)
               THEN ('$100,000,000-$499,999,999')
           WHEN (location_sales_volume >= 500000000 AND location_sales_volume <= 999999999)
               THEN ('$500,000,000-$999,999,999')
           WHEN (location_sales_volume >= 1000000000) THEN ('$1,000,000,000+')
           END                      location_sales_volume,
       CASE
           WHEN (estimated_corporate_sales_revenue < 50000) THEN ('$0-$499,999')
           WHEN (estimated_corporate_sales_revenue >= 50000 AND estimated_corporate_sales_revenue <= 999999)
               THEN ('$500,$000-999,999')
           WHEN (estimated_corporate_sales_revenue >= 1000000 AND estimated_corporate_sales_revenue <= 2499999)
               THEN ('$1,000,000-$2,499,999')
           WHEN (estimated_corporate_sales_revenue >= 2500000 AND estimated_corporate_sales_revenue <= 4999999)
               THEN ('$2,500,000-$4,999,999')
           WHEN (estimated_corporate_sales_revenue >= 5000000 AND estimated_corporate_sales_revenue <= 9999999)
               THEN ('$5,000,000-$9,999,999')
           WHEN (estimated_corporate_sales_revenue >= 10000000 AND estimated_corporate_sales_revenue <= 19999999)
               THEN ('$10,000,000-$19,999,999')
           WHEN (estimated_corporate_sales_revenue >= 20000000 AND estimated_corporate_sales_revenue <= 49999999)
               THEN ('$20,000,000-$49,999,999')
           WHEN (estimated_corporate_sales_revenue >= 50000000 AND estimated_corporate_sales_revenue <= 99999999)
               THEN ('$50,000,000-$99,999,999')
           WHEN (estimated_corporate_sales_revenue >= 100000000 AND estimated_corporate_sales_revenue <= 499999999)
               THEN ('$100,000,000-$499,999,999')
           WHEN (estimated_corporate_sales_revenue >= 500000000 AND estimated_corporate_sales_revenue <= 999999999)
               THEN ('$500,000,000-$999,999,999')
           WHEN (estimated_corporate_sales_revenue >= 1000000000) THEN ('$1,000,000,000+')
           END                      estimated_corporate_sales_revenue,
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
           END                      location_employee_size,
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
           END                      corporate_employee_size,
       CASE
           WHEN (estimated_location_employee_count >= 1 AND estimated_location_employee_count <= 4) THEN ('1-4')
           WHEN (estimated_location_employee_count >= 5 AND estimated_location_employee_count <= 9) THEN ('5-9')
           WHEN (estimated_location_employee_count >= 10 AND estimated_location_employee_count <= 19) THEN ('10-19')
           WHEN (estimated_location_employee_count >= 20 AND estimated_location_employee_count <= 49) THEN ('20-49')
           WHEN (estimated_location_employee_count >= 50 AND estimated_location_employee_count <= 99) THEN ('50-99')
           WHEN (estimated_location_employee_count >= 100 AND estimated_location_employee_count <= 249) THEN ('100-249')
           WHEN (estimated_location_employee_count >= 200 AND estimated_location_employee_count <= 499) THEN ('250-499')
           WHEN (estimated_location_employee_count >= 500 AND estimated_location_employee_count <= 999) THEN ('500-999')
           WHEN (estimated_location_employee_count >= 1000 AND estimated_location_employee_count <= 4999)
               THEN ('1,000-4,999')
           WHEN (estimated_location_employee_count >= 5000 AND estimated_location_employee_count <= 9999)
               THEN ('5,000-9,999')
           WHEN (estimated_location_employee_count >= 10000) THEN ('10,000+')
           END                      estimated_location_employee_size,
       CASE
           WHEN (estimated_corporate_employee_count >= 1 AND estimated_corporate_employee_count <= 4) THEN ('1-4')
           WHEN (estimated_corporate_employee_count >= 5 AND estimated_corporate_employee_count <= 9) THEN ('5-9')
           WHEN (estimated_corporate_employee_count >= 10 AND estimated_corporate_employee_count <= 19) THEN ('10-19')
           WHEN (estimated_corporate_employee_count >= 20 AND estimated_corporate_employee_count <= 49) THEN ('20-49')
           WHEN (estimated_corporate_employee_count >= 50 AND estimated_corporate_employee_count <= 99) THEN ('50-99')
           WHEN (estimated_corporate_employee_count >= 100 AND estimated_corporate_employee_count <= 249)
               THEN ('100-249')
           WHEN (estimated_corporate_employee_count >= 200 AND estimated_corporate_employee_count <= 499)
               THEN ('250-499')
           WHEN (estimated_corporate_employee_count >= 500 AND estimated_corporate_employee_count <= 999)
               THEN ('500-999')
           WHEN (estimated_corporate_employee_count >= 1000 AND estimated_corporate_employee_count <= 4999)
               THEN ('1,000-4,999')
           WHEN (estimated_corporate_employee_count >= 5000 AND estimated_corporate_employee_count <= 9999)
               THEN ('5,000-9,999')
           WHEN (estimated_corporate_employee_count >= 10000) THEN ('10,000+')
           END                      Estimated_corporate_employee_size,
       CASE
           WHEN (website = '') THEN ('false')
           ELSE ('true')
           END                      website_ind,
       CASE
           WHEN (facebook_url = '') THEN ('false')
           ELSE ('true')
           END                      facebook_url_ind,
       CASE
           WHEN (twitter_url = '') THEN ('false')
           ELSE ('true')
           END                      twitter_url_ind,
       CASE
           WHEN (linked_in_url = '') THEN ('false')
           ELSE ('true')
           END                      linked_in_url_ind,
       CASE
           WHEN (yelp_url = '') THEN ('false')
           ELSE ('true')
           END                      yelp_url_ind,
       CASE
           WHEN (pinterest_url = '') THEN ('false')
           ELSE ('true')
           END                      pinterest_url_ind,
       CASE
           WHEN (youtube_url = '') THEN ('false')
           ELSE ('true')
           END                      youtube_url_ind,
       CASE
           WHEN (tumblr_url = '') THEN ('false')
           ELSE ('true')
           END                      tumblr_url_ind,
       CASE
           WHEN (foursquare_url = '') THEN ('false')
           ELSE ('true')
           END                      foursquare_url_ind,
       CASE
           WHEN (instagram_url = '') THEN ('false')
           ELSE ('true')
           END                      instagram_url_ind,
       CASE
           WHEN (logo_url = '') THEN ('false')
           ELSE ('true')
           END                      logo_url_ind,
       -- Asha- Starts the phase 2 filters.
       -- SUBSTRING(primary_sic_code_id,1,4) restaurant_hotel_sic,
       -- substring(primary_sic_code,1,2) hotel_sic,
       CASE
           WHEN restaurant_cuisines = '' THEN ('false')
           ELSE ('True') END        restaurant_cuisines,
       --The below elements have true, false and null, so have called the variable as is.
       restaurant_limited_service,
       restaurant_reservations,
       restaurant_takeout,
       --The below element has null, 0 , 1+. Hence 1+ is true, rest are false- can be used in place of happy_hours_id. (same count)

       CASE
           WHEN (infogroup_id_happy_hour_id = 1) THEN ('true')
           ELSE ('false')
           END                      happy_hour_id,

       CASE
           WHEN (happy_hours_count >= 1) THEN ('true')
           ELSE ('false')
           END                      happy_hours_count,
       CASE
           WHEN (hotel_id = '') THEN ('false')
           ELSE ('true')
           END                      hotel_id,
       -- These below elements have false, true and null, hence called the var as is.
       hotel_cable_tv,
       hotel_continental_breakfast,
       hotel_elevator,
       hotel_exercise_facility,
       hotel_guest_laundry,
       hotel_hot_tub,
       hotel_indoor_pool,
       hotel_kitchens,
       hotel_outdoor_pool,
       hotel_pet_friendly,
       hotel_room_service,
       --infogroup_id,
       b.infogroup_id,

       --Transformation of elements for Phase 1.

---Broad groups-core information, Categories-Names
       CASE
           WHEN name = '' THEN NULL
           ELSE 1
           END                      infogroup_id_name,
       ---Broad groups-core information, Categories- Addresses
       CASE
           WHEN street = '' THEN NULL
           ELSE 1
           END                      infogroup_id_street,
       CASE
           WHEN suite = '' THEN NULL
           ELSE 1
           END                      infogroup_id_suite,
       CASE
           WHEN city = '' THEN NULL
           ELSE 1
           END                      infogroup_id_city,
       CASE
           WHEN state = '' THEN NULL
           ELSE 1
           END                      infogroup_id_state,
       CASE
           WHEN postal_code = '' THEN NULL
           ELSE 1
           END                      infogroup_id_postal_code,

       ---Broad groups-core information, Categories- Categories
       ---Need to add tags.
       CASE
           WHEN primary_sic_code_id = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_sic_code_id,
       CASE
           WHEN sic_code_ids = '' THEN NULL
           ELSE 1
           END                      infogroup_id_sic_code_ids,
       CASE
           WHEN primary_naics_code_id = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_naics_code_id,
       CASE
           WHEN naics_code_ids = '' THEN NULL
           ELSE 1
           END                      infogroup_id_naics_code_ids,

---Broad groups-core information, Categories- Contact Info
       CASE
           WHEN phone = '' THEN NULL
           ELSE 1
           END                      infogroup_id_phone,
       ---Broad groups-core information, Categories- Websites

       CASE
           WHEN website = '' THEN NULL
           ELSE 1
           END                      infogroup_id_website,
       --Need to add website status not in places table.
       --CASE
       -- WHEN website_status = '' THEN NULL
       -- ELSE b.infogroup_id
       -- END infogroup_id_website_status,
       CASE
           WHEN facebook_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_facebook_url,
       CASE
           WHEN twitter_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_twitter_url,
       CASE
           WHEN linked_in_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_linked_in_url,
       CASE
           WHEN yelp_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_yelp_url,
       CASE
           WHEN pinterest_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_pinterest_url,
       CASE
           WHEN youtube_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_youtube_url,
       CASE
           WHEN tumblr_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_tumblr_url,
       CASE
           WHEN foursquare_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_foursquare_url,
       CASE
           WHEN instagram_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_instagram_url,
       CASE
           WHEN logo_url = '' THEN NULL
           ELSE 1
           END                      infogroup_id_logo_url,
       ---bbb_business_review_url and single_platform_url not there on places.
    /*This is from Phase 5 as it belongs to websites categories. */
       CASE
           WHEN website_keywords = '' THEN 0
           ELSE 1
           END                      infogroup_id_website_keywords,

       ---Broad groups-core information, Categories- Employees
       CASE
           WHEN primary_contact_id = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_id,
       CASE
           WHEN primary_contact_email = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_email,
       CASE
           WHEN primary_contact_email_md5 = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_email_md5,
       CASE
           WHEN primary_contact_email_sha256 = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_email_sha256,
       CASE
           WHEN primary_contact_email_vendor_id = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_email_vendor_id,
       CASE
           WHEN primary_contact_email_deliverable = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_email_deliverable,
       CASE
           WHEN primary_contact_email_marketable = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_email_marketable,
       CASE
           WHEN primary_contact_email_reputation_risk = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_email_reputation_risk,
       CASE
           WHEN primary_contact_first_name = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_first_name,
       CASE
           WHEN primary_contact_gender = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_gender,
       CASE
           WHEN primary_contact_job_function_id IS NOT NULL THEN 1
           END                      infogroup_id_primary_contact_job_function_id,
       CASE
           WHEN primary_contact_job_titles = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_job_titles,
       CASE
           WHEN primary_contact_job_title_ids = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_job_title_ids,
       CASE
           WHEN primary_contact_last_name = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_last_name,
       CASE
           WHEN primary_contact_management_level = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_management_level,
       CASE
           WHEN primary_contact_mapped_contact_id = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_mapped_contact_id,
       CASE
           WHEN primary_contact_professional_title = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_professional_title,
       --  CASE
       --  WHEN primary_contact_primary = '' THEN NULL
       --ELSE 1
       --END infogroup_id_primary_contact_primary,
       CASE
           WHEN primary_contact_title_codes = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_title_codes,
       CASE
           WHEN primary_contact_vendor_id = '' THEN NULL
           ELSE 1
           END                      infogroup_id_primary_contact_vendor_id,
       CASE
           WHEN (location_employee_count >= 1) THEN 1
           ELSE 0
           END                      infogroup_id_location_employee_count,
       CASE
           WHEN (estimated_location_employee_count >= 1) THEN 1
           ELSE 0
           END                      infogroup_id_estimated_location_employee_count,
       CASE
           WHEN (corporate_employee_count >= 1) THEN 1
           ELSE 0
           END                      infogroup_id_corporate_employee_count,
       --This below var is from phase 4 and is brought into phase 1& 2 due to it belongs to categories=employees.
       CASE
           WHEN location_professional_size_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_location_professional_size_code,

       CASE
           WHEN ownership_changed_on = '' THEN 0
           ELSE 1
           END                      infogroup_id_ownership_changed_on,

       CASE
           WHEN (estimated_corporate_employee_count >= 1) THEN 1
           ELSE 0
           END                      infogroup_id_estimated_corporate_employee_count,
       CASE
           WHEN corporate_sales_revenue IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_corporate_sales_revenue,
       CASE
           WHEN estimated_corporate_sales_revenue IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_estimated_corporate_sales_revenue,
       CASE
           WHEN location_sales_volume IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_location_sales_volume,
--case when (place_type='headquarters') then (1) else (0)
--end infogroup_id_headquarters_id,
       infogroup_id_adsize_code_tags,
       infogroup_id_primary_tags,
       infogroup_id_sic_code_id_tags,
       infogroup_id_naics_code_id_tags,
       infogroup_id_yellow_page_code_tags,
       infogroup_id_yppa_code_tags,

--Transformation of elements/Measures begin for Phase 2
--Broad groups-core information, Categories-Identifiers and Classifiers

       CASE
           WHEN place_type = '' THEN 0
           ELSE 1
           END                      infogroup_id_place_type,
       CASE
           WHEN work_at_home = '' THEN 0
           ELSE 1
           END                      infogroup_id_work_at_home,
       ---Broad groups-core information, Categories-Status & Recency
       --CASE
       --WHEN in_business = '' THEN 0
       --ELSE 1
       --END infogroup_id_in_business,
       CASE
           WHEN verified_on = '' THEN 0
           ELSE 1
           END                      infogroup_id_verified_on,
       -- CASE
       --  WHEN listing_status = '' THEN 0
       --   ELSE 1
       -- END infogroup_id_listing_status,
       -- CASE
       --   WHEN operating_status = '' THEN 0
       --   ELSE 1
       --END infogroup_id_operating_status,
       CASE
           WHEN created_at = '' THEN 0
           ELSE 1
           END                      infogroup_id_created_at,
       CASE
           WHEN updated_at = '' THEN 0
           ELSE 1
           END                      infogroup_id_updated_at,
       CASE
           WHEN in_business_on = '' THEN 0
           ELSE 1
           END                      infogroup_id_in_business_on,
       CASE
           WHEN opened_for_business_on = '' THEN 0
           ELSE 1
           END                      infogroup_id_opened_for_business_on,
       --Here i have created 2 vars.
       CASE
           WHEN estimated_opened_for_business_lower = '' THEN 0
           ELSE 1
           END                      infogroup_id_estimated_opened_for_business_lower,
       CASE
           WHEN estimated_opened_for_business_upper = '' THEN 0
           ELSE 1
           END                      infogroup_id_estimated_opened_for_business_upper,
       CASE
           WHEN express_updated_at = '' THEN 0
           ELSE 1
           END                      infogroup_id_express_updated_at,
       CASE
           WHEN bulk_updated_at = '' THEN 0
           ELSE 1
           END                      infogroup_id_bulk_updated_at,

---Broad groups-core information, Categories-Names
       CASE
           WHEN alternative_name = '' THEN 0
           ELSE 1
           END                      infogroup_id_alternative_name,
       CASE
           WHEN historical_names = '' THEN 0
           ELSE 1
           END                      infogroup_id_historical_names,
       CASE
           WHEN legal_names = '' THEN 0
           ELSE 1
           END                      infogroup_id_legal_names,
       ---Broad groups-core information, Categories- Addresses

       CASE
           WHEN country_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_country_code,
    /*This below field is part of phase 6 -included here as it belongs  to the category address*/
       CASE
           WHEN territory = '' THEN 0
           ELSE 1
           END                      infogroup_id_territory,
       CASE
           WHEN cross_street_address = '' THEN 0
           ELSE 1
           END                      infogroup_id_cross_street_address,
       CASE
           WHEN address_changed_on = '' THEN 0
           ELSE 1
           END                      infogroup_id_address_changed_on,
       CASE
           WHEN mailing_address = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_address,
       CASE
           WHEN mailing_address_city = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_address_city,
       CASE
           WHEN mailing_address_state = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_address_state,
       CASE
           WHEN mailing_address_postal_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_address_postal_code,
       CASE
           WHEN latitude IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_latitude,
       CASE
           WHEN longitude IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_longitude,
       CASE
           WHEN geo_match_level = '' THEN 0
           ELSE 1
           END                      infogroup_id_geo_match_level,
       -- Here feel free to use either geocoordinate_lat / long same with manual_lat & Long.
       --  They return same values.
       CASE
           WHEN geocoordinate_lat IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_geocoordinate_lat,
       CASE
           WHEN geocoordinate_lon IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_geocoordinate_lon,
       CASE
           WHEN manual_geocoordinate_lat IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_manual_geocoordinate_lat,
       CASE
           WHEN manual_geocoordinate_lon IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_manual_geocoordinate_lon,
       ---Broad groups-core information, Categories- Categories.
       CASE
           WHEN business_type_ids = '' THEN 0
           ELSE 1
           END                      infogroup_id_business_type_ids,
       ---Broad groups-core information, Categories- Contact Info
       CASE
           WHEN toll_free_number = '' THEN 0
           ELSE 1
           END                      infogroup_id_toll_free_number,
       CASE
           WHEN fax_number = '' THEN 0
           ELSE 1
           END                      infogroup_id_fax_number,
       CASE
           WHEN additional_phone = '' THEN 0
           ELSE 1
           END                      infogroup_id_additional_phone,
       CASE
           WHEN phone_call_status_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_phone_call_status_code,
       CASE
           WHEN teleresearch_update_date = '' THEN 0
           ELSE 1
           END                      infogroup_id_teleresearch_update_date,
       CASE
           WHEN location_email_address = '' THEN 0
           ELSE 1
           END                      infogroup_id_location_email_address,


       --Broad groups-core information, Categories- Headquarters details

       CASE
           WHEN headquarters_id = '' THEN 0
           ELSE 1
           END                      infogroup_id_headquarters_id,
       CASE
           WHEN ancestor_headquarters_ids = '' THEN 0
           ELSE 1
           END                      infogroup_id_ancestor_headquarters_ids,
       CASE
           WHEN ultimate_headquarters_id = '' THEN 0
           ELSE 1
           END                      infogroup_id_ultimate_headquarters_id,
       CASE
           WHEN chain_id = '' THEN 0
           ELSE 1
           END                      infogroup_id_chain_id,
       --  CASE
       --  WHEN branch_type_id = '' THEN 0
       --ELSE 1
       --END infogroup_id_branch_type_id,
       CASE
           WHEN corporate_franchising = '' THEN 0
           ELSE 1
           END                      infogroup_id_corporate_franchising,
       CASE
           WHEN branch_count >= 1 THEN 1
           ELSE 0
           END                      infogroup_id_branch_count,
       CASE
           WHEN foreign_parent_flag = '' THEN 0
           ELSE 1
           END                      infogroup_id_foreign_parent_flag,
       CASE
           WHEN fortune_ranking IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_fortune_ranking,
       CASE
           WHEN fiscal_year_end_month = '' THEN 0
           ELSE 1
           END                      infogroup_id_fiscal_year_end_month,
       CASE
           WHEN stock_exchange_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_stock_exchange_code,
       CASE
           WHEN stock_ticker_symbol = '' THEN 0
           ELSE 1
           END                      infogroup_id_stock_ticker_symbol,
       CASE
           WHEN cik = '' THEN 0
           ELSE 1
           END                      infogroup_id_cik,

       CASE
           WHEN eins_count >= 1 THEN 1
           ELSE 0
           END                      infogroup_id_eins_count,

       --Broad groups: Places insights Categories: Business.
       CASE
           WHEN affiliation_ids = '' THEN 0
           ELSE 1
           END                      infogroup_id_affiliation_ids,
       CASE
           WHEN brand_ids = '' THEN 0
           ELSE 1
           END                      infogroup_id_brand_ids,
       --operating hours not part of core_bf
       CASE
           WHEN payment_types = '' THEN 0
           ELSE 1
           END                      infogroup_id_payment_types,
       CASE
           WHEN car_make_ids = '' THEN 0
           ELSE 1
           END                      infogroup_id_car_make_ids,
       CASE
           WHEN religious_denomination_ids = '' THEN 0
           ELSE 1
           END                      infogroup_id_religious_denomination_ids,

--categories-Business

       CASE
           WHEN greenscore IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_greenscore,
       CASE
           WHEN growing_business_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_growing_business_code,
       CASE
           WHEN white_collar_percentage IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_white_collar_percentage,
       --Broad groups: Specific category  Categories: Eating/Drinking.
--Here restaurant cusisine has multiple cusisines and is null or cuisines.
       CASE
           WHEN restaurant_cuisines = '' THEN 0
           ELSE 1
           END                      infogroup_id_restaurant_cuisines,
--Here limited service, reservation, take out  has true,false and null as values
       CASE
           WHEN restaurant_limited_service = '' THEN 0
           ELSE 1
           END                      infogroup_id_restaurant_limited_service,
       CASE
           WHEN restaurant_reservations = '' THEN 0
           ELSE 1
           END                      infogroup_id_restaurant_reservations,
       CASE
           WHEN restaurant_takeout = '' THEN 0
           ELSE 1
           END                      infogroup_id_restaurant_takeout,

--Happy hours data is not there in core_bf however as a separate table see above.
--On Hotel apart from hotel_created_at, the rest of the elements have true, false and missing
       CASE
           WHEN hotel_cable_tv = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_cable_tv,
       CASE
           WHEN hotel_continental_breakfast = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_continental_breakfast,
       CASE
           WHEN hotel_elevator = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_elevator,
       CASE
           WHEN hotel_exercise_facility = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_exercise_facility,
       CASE
           WHEN hotel_guest_laundry = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_guest_laundry,
       CASE
           WHEN hotel_hot_tub = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_hot_tub,
       CASE
           WHEN hotel_indoor_pool = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_indoor_pool,
       CASE
           WHEN hotel_kitchens = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_kitchens,
       CASE
           WHEN hotel_outdoor_pool = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_outdoor_pool,
       CASE
           WHEN hotel_pet_friendly = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_pet_friendly,
       CASE
           WHEN hotel_room_service = '' THEN 0
           ELSE 1
           END                      infogroup_id_hotel_room_service,
       CASE
           WHEN (happy_hours_count >= 1) THEN 1
           ELSE 0
           END                      infogroup_id_happy_hours_count,
       infogroup_id_op_start_time,
       infogroup_id_op_end_time,
       infogroup_id_op_days,
       infogroup_id_happy_hour_special_food,
       infogroup_id_happy_hour_special_drink,
       infogroup_id_happy_hour_special_activity,
       infogroup_id_happy_hour_special_other,
       infogroup_id_happy_hour_start_time,
       infogroup_id_happy_hour_end_time,
       infogroup_id_happy_hour_days
INTO reports.tableau_raw_data_phase12
FROM core_bf.v_places_in_business b
--FROM reports.mv_places_in_business b
         LEFT JOIN reports.tableau_tags_data_final t
                   ON (t.infogroup_id = b.infogroup_id)
         LEFT JOIN reports.professional_title_lk pt
                   ON (b.primary_contact_professional_title = pt.professional_title)
         LEFT JOIN reports.tableau_op_data_final o
                   ON (o.infogroup_id = b.infogroup_id)
         LEFT JOIN reports.tableau_happy_data_final h
                   ON (h.infogroup_id = b.infogroup_id)
WHERE in_business = 'yes';

--select count(*) from core_bf.v_places_in_business

--17063817
--select estimated_corporate_employee_size  from reports.tableau_raw_data_phase12 limit 10;
-- 16861345

DROP TABLE IF EXISTS reports.tableau_extract_phase12;

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
       restaurant_cuisines,
       restaurant_limited_service,
       restaurant_reservations,
       restaurant_takeout,
       happy_hours_count,
       happy_hour_id,
       hotel_id,
       hotel_cable_tv,
       hotel_continental_breakfast,
       hotel_elevator,
       hotel_exercise_facility,
       hotel_guest_laundry,
       hotel_hot_tub,
       hotel_indoor_pool,
       hotel_kitchens,
       hotel_outdoor_pool,
       hotel_pet_friendly,
       hotel_room_service,
       COUNT(infogroup_id)                                     cnt,
       SUM(infogroup_id_name)                                  cnt_name,
       SUM(infogroup_id_street)                                cnt_street,
       SUM(infogroup_id_suite)                                 cnt_suite,
       SUM(infogroup_id_city)                                  cnt_city,
       SUM(infogroup_id_state)                                 cnt_state,
       SUM(infogroup_id_postal_code)                           cnt_postal_code,
       SUM(infogroup_id_primary_sic_code_id)                   cnt_primary_sic_code_id,
       SUM(infogroup_id_sic_code_ids)                          cnt_sic_code_ids,
       SUM(infogroup_id_primary_naics_code_id)                 cnt_primary_naics_code_id,
       SUM(infogroup_id_naics_code_ids)                        cnt_naics_code_ids,
       SUM(infogroup_id_phone)                                 cnt_phone,
       SUM(infogroup_id_website)                               cnt_website,
       --SUM(infogroup_id_website_status) cnt_website_status,
       SUM(infogroup_id_facebook_url)                          cnt_facebook_url,
       SUM(infogroup_id_twitter_url)                           cnt_twitter_url,
       SUM(infogroup_id_linked_in_url)                         cnt_linked_in_url,
       SUM(infogroup_id_yelp_url)                              cnt_yelp_url,
       SUM(infogroup_id_pinterest_url)                         cnt_pinterest_url,
       SUM(infogroup_id_youtube_url)                           cnt_youtube_url,
       SUM(infogroup_id_tumblr_url)                            cnt_tumblr_url,
       SUM(infogroup_id_foursquare_url)                        cnt_foursquare_url,
       SUM(infogroup_id_instagram_url)                         cnt_instagram_url,
       SUM(infogroup_id_logo_url)                              cnt_logo_url,
       SUM(infogroup_id_website_keywords)                      cnt_website_keywords,
       --SUM(infogroup_id_bbb_business_review_url) cnt_bbb_business_review_url,
       --SUM(infogroup_id_single_platform_url) cnt_single_platform_url,
       SUM(infogroup_id_primary_contact_id)                    cnt_primary_contact_id,
       SUM(infogroup_id_primary_contact_email)                 cnt_primary_contact_email,
       SUM(infogroup_id_primary_contact_email_md5)             cnt_primary_contact_email_md5,
       SUM(infogroup_id_primary_contact_email_sha256)          cnt_primary_contact_email_sha256,
       SUM(infogroup_id_primary_contact_email_vendor_id)       cnt_primary_contact_email_vendor_id,
       SUM(infogroup_id_primary_contact_email_deliverable)     cnt_primary_contact_email_deliverable,
       SUM(infogroup_id_primary_contact_email_marketable)      cnt_primary_contact_email_marketable,
       SUM(infogroup_id_primary_contact_email_reputation_risk) cnt_primary_contact_email_reputation_risk,
       SUM(infogroup_id_primary_contact_first_name)            cnt_primary_contact_first_name,
       SUM(infogroup_id_primary_contact_gender)                cnt_primary_contact_gender,
       SUM(infogroup_id_primary_contact_job_function_id)       cnt_primary_contact_job_function_id,
       SUM(infogroup_id_primary_contact_job_titles)            cnt_primary_contact_job_titles,
       SUM(infogroup_id_primary_contact_job_title_ids)         cnt_primary_contact_job_title_ids,
       SUM(infogroup_id_primary_contact_last_name)             cnt_primary_contact_last_name,
       SUM(infogroup_id_primary_contact_management_level)      cnt_primary_contact_management_level,
       SUM(infogroup_id_primary_contact_mapped_contact_id)     cnt_primary_contact_mapped_contact_id,
       SUM(infogroup_id_primary_contact_professional_title)    cnt_primary_contact_professional_title,
       -- SUM(infogroup_id_primary_contact_primary) cnt_primary_contact_primary,
       SUM(infogroup_id_primary_contact_title_codes)           cnt_primary_contact_title_codes,
       SUM(infogroup_id_primary_contact_vendor_id)             cnt_primary_contact_vendor_id,
       SUM(infogroup_id_location_employee_count)               cnt_location_employee_count,
       SUM(infogroup_id_estimated_location_employee_count)     cnt_estimated_location_employee_count,
       SUM(infogroup_id_location_professional_size_code)       cnt_location_professional_size_code,
       SUM(infogroup_id_ownership_changed_on)                  cnt_ownership_changed_on,
       SUM(infogroup_id_corporate_employee_count)              cnt_corporate_employee_count,
       SUM(infogroup_id_estimated_corporate_employee_count)    cnt_estimated_corporate_employee_count,
       SUM(infogroup_id_corporate_sales_revenue)               cnt_corporate_sales_revenue,
       SUM(infogroup_id_estimated_corporate_sales_revenue)     cnt_estimated_corporate_sales_revenue,
       SUM(infogroup_id_location_sales_volume)                 cnt_location_sales_volume,
       --sum(infogroup_id_headquarters_id) cnt_headquarters_id,
       SUM(infogroup_id_adsize_code_tags)                      cnt_adsize_code_tags,
       SUM(infogroup_id_primary_tags)                          cnt_primary_tags,
       SUM(infogroup_id_sic_code_id_tags)                      cnt_sic_code_id_tags,
       SUM(infogroup_id_naics_code_id_tags)                    cnt_naics_code_id_tags,
       SUM(infogroup_id_yellow_page_code_tags)                 cnt_yellow_page_code_tags,
       SUM(infogroup_id_yppa_code_tags)                        cnt_id_yppa_code_tags,
       --Phase 2 begins
       SUM(infogroup_id_place_type)                            cnt_place_type,
       SUM(infogroup_id_work_at_home)                          cnt_work_at_home,
       --   SUM(infogroup_id_in_business) cnt_in_business,
       SUM(infogroup_id_verified_on)                           cnt_verified_on,
       --SUM(infogroup_id_listing_status) cnt_listing_status,
       --SUM(infogroup_id_operating_status) cnt_operating_status,
       SUM(infogroup_id_created_at)                            cnt_created_at,
       SUM(infogroup_id_updated_at)                            cnt_updated_at,
       SUM(infogroup_id_in_business_on)                        cnt_in_business_on,
       SUM(infogroup_id_opened_for_business_on)                cnt_opened_for_business_on,
       SUM(infogroup_id_estimated_opened_for_business_lower)   cnt_estimated_opened_for_business_on_lower,
       SUM(infogroup_id_estimated_opened_for_business_upper)   cnt_estimated_opened_for_business_on_upper,
       SUM(infogroup_id_express_updated_at)                    cnt_express_updated_at,
       SUM(infogroup_id_bulk_updated_at)                       cnt_bulk_updated_at,
       SUM(infogroup_id_alternative_name)                      cnt_alternative_name,
       SUM(infogroup_id_historical_names)                      cnt_historical_names,
       SUM(infogroup_id_legal_names)                           cnt_legal_names,
       SUM(infogroup_id_country_code)                          cnt_country_code,
       SUM(infogroup_id_territory)                             cnt_territory,
       SUM(infogroup_id_cross_street_address)                  cnt_cross_street_address,
       SUM(infogroup_id_address_changed_on)                    cnt_address_changed_on,
       SUM(infogroup_id_mailing_address)                       cnt_mailing_address,
       SUM(infogroup_id_mailing_address_city)                  cnt_mailing_address_city,
       SUM(infogroup_id_mailing_address_state)                 cnt_mailing_address_state,
       SUM(infogroup_id_mailing_address_postal_code)           cnt_mailing_address_postal_code,
       SUM(infogroup_id_latitude)                              cnt_latitude,
       SUM(infogroup_id_longitude)                             cnt_longitude,
       SUM(infogroup_id_geo_match_level)                       cnt_geo_match_level,
       SUM(infogroup_id_geocoordinate_lat)                     cnt_geocoordinate_lat,
       SUM(infogroup_id_geocoordinate_lon)                     cnt_geocoordinate_lon,
       SUM(infogroup_id_manual_geocoordinate_lat)              cnt_manual_geocoordinate_lat,
       SUM(infogroup_id_manual_geocoordinate_lon)              cnt_manual_geocoordinate_lon,
       SUM(infogroup_id_business_type_ids)                     cnt_business_type_ids,
       SUM(infogroup_id_toll_free_number)                      cnt_toll_free_number,
       SUM(infogroup_id_fax_number)                            cnt_fax_number,
       SUM(infogroup_id_additional_phone)                      cnt_additional_phone,
       SUM(infogroup_id_phone_call_status_code)                cnt_phone_call_status_code,
       SUM(infogroup_id_teleresearch_update_date)              cnt_teleresearch_update_date,
       SUM(infogroup_id_location_email_address)                cnt_location_email_address,
       SUM(infogroup_id_headquarters_id)                       cnt_headquarters_id,
       SUM(infogroup_id_ancestor_headquarters_ids)             cnt_ancestor_headquarters_ids,
       SUM(infogroup_id_ultimate_headquarters_id)              cnt_ultimate_headquarters_id,
       SUM(infogroup_id_chain_id)                              cnt_chain_id,
       SUM(infogroup_id_corporate_franchising)                 cnt_corporate_franchising,
       SUM(infogroup_id_branch_count)                          cnt_branch_count,
       SUM(infogroup_id_foreign_parent_flag)                   cnt_foreign_parent_flag,
       SUM(infogroup_id_fortune_ranking)                       cnt_fortune_ranking,
       SUM(infogroup_id_fiscal_year_end_month)                 cnt_fiscal_year_end_month,
       SUM(infogroup_id_stock_exchange_code)                   cnt_stock_exchange_code,
       SUM(infogroup_id_stock_ticker_symbol)                   cnt_stock_ticker_symbol,
       SUM(infogroup_id_cik)                                   cnt_cik,
       SUM(infogroup_id_eins_count)                            cnt_eins_count,
       SUM(infogroup_id_affiliation_ids)                       cnt_affiliation_ids,
       SUM(infogroup_id_brand_ids)                             cnt_brand_ids,
       SUM(infogroup_id_payment_types)                         cnt_payment_types,
       SUM(infogroup_id_car_make_ids)                          cnt_car_make_ids,
       SUM(infogroup_id_religious_denomination_ids)            cnt_religious_denomination_ids,
       SUM(infogroup_id_greenscore)                            cnt_greenscore,
       SUM(infogroup_id_growing_business_code)                 cnt_growing_business_code,
       SUM(infogroup_id_white_collar_percentage)               cnt_white_collar_percentage,
       SUM(infogroup_id_restaurant_cuisines)                   cnt_restaurant_cuisines,
       SUM(infogroup_id_restaurant_limited_service)            cnt_restaurant_limited_service,
       SUM(infogroup_id_restaurant_reservations)               cnt_restaurant_reservations,
       SUM(infogroup_id_restaurant_takeout)                    cnt_restaurant_takeout,
       SUM(infogroup_id_hotel_cable_tv)                        cnt_hotel_cable_tv,
       SUM(infogroup_id_hotel_continental_breakfast)           cnt_hotel_continental_breakfast,
       SUM(infogroup_id_hotel_elevator)                        cnt_hotel_elevator,
       SUM(infogroup_id_hotel_exercise_facility)               cnt_hotel_exercise_facility,
       SUM(infogroup_id_hotel_guest_laundry)                   cnt_hotel_guest_laundry,
       SUM(infogroup_id_hotel_hot_tub)                         cnt_hotel_hot_tub,
       SUM(infogroup_id_hotel_indoor_pool)                     cnt_hotel_indoor_pool,
       SUM(infogroup_id_hotel_kitchens)                        cnt_hotel_kitchens,
       SUM(infogroup_id_hotel_outdoor_pool)                    cnt_hotel_outdoor_pool,
       SUM(infogroup_id_hotel_pet_friendly)                    cnt_hotel_pet_friendly,
       SUM(infogroup_id_hotel_room_service)                    cnt_hotel_room_service,
       SUM(infogroup_id_happy_hours_count)                     cnt_happy_hours_count,
       SUM(infogroup_id_op_start_time)                         cnt_op_start_time,
       SUM(infogroup_id_op_end_time)                           cnt_op_end_time,
       SUM(infogroup_id_op_days)                               cnt_op_days,
       SUM(infogroup_id_happy_hour_special_food)               cnt_happy_hour_special_food,
       SUM(infogroup_id_happy_hour_special_drink)              cnt_happy_hour_special_drink,
       SUM(infogroup_id_happy_hour_special_activity)           cnt_happy_hour_special_activity,
       SUM(infogroup_id_happy_hour_special_other)              cnt_happy_hour_special_other,
       SUM(infogroup_id_happy_hour_start_time)                 cnt_happy_hour_start_time,
       SUM(infogroup_id_happy_hour_end_time)                   cnt_happy_hour_end_time,
       SUM(infogroup_id_happy_hour_days)                       cnt_happy_hour_days
INTO reports.tableau_extract_phase12
FROM reports.tableau_raw_data_phase12
GROUP BY place_type,
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
         restaurant_cuisines,
         restaurant_limited_service,
         restaurant_reservations,
         restaurant_takeout,
         happy_hours_count,
         happy_hour_id,
         hotel_id,
         hotel_cable_tv,
         hotel_continental_breakfast,
         hotel_elevator,
         hotel_exercise_facility,
         hotel_guest_laundry,
         hotel_hot_tub,
         hotel_indoor_pool,
         hotel_kitchens,
         hotel_outdoor_pool,
         hotel_pet_friendly,
         hotel_room_service;

/*

select primary_contact_professional_title, count(1),sum(cnt),
SUM(cnt_adsize_code_tags),
       SUM(cnt_primary_tags) cnt_primary_tags,
      SUM(cnt_sic_code_id_tags) cnt_sic_code_id,
       SUM(cnt_naics_code_id_tags) cnt_naics_code_id,
       SUM(cnt_yellow_page_code_tags) cnt_yellow_page_code,
       SUM(cnt_id_yppa_code_tags) from reports.tableau_extract_phase12
       group by primary_contact_professional_title;

-- 13,120,951
-- 13,351,657

select primary_contact, count(1) from reports.tableau_extract_phase12
group by primary_contact
false	4238837
true	9152975

select primary_contact, count(1) from reports.tableau_extract_phase12
group by primary_contact
--true	9116130

select restaurant_cuisines, count(1) from reports.tableau_extract_phase12
group by restaurant_cuisines

--True	526809

select  hotel_cable_tv, count(1) from reports.tableau_raw_data_phase12
group by  hotel_cable_tv
true	30335
false	1228
	16952012

select  infogroup_id_branch_count, count(1) from reports.tableau_raw_data_phase12
group by infogroup_id_branch_count
-1	62089
select infogroup_id_op_end_time, count(1) from reports.tableau_raw_data_phase12
group by infogroup_id_op_end_time

	11844603
1	5138972

select  infogroup_id_branch_count, count(1) from reports.tableau_raw_data_phase12
group by infogroup_id_branch_count

select infogroup_id_happy_hour_id,, count(1) from reports.tableau_raw_data_phase12
group by infogroup_id_happy_hours_count

select infogroup_id_happy_hour_id, count(1) from reports.tableau_happy_data_final
group by infogroup_id_happy_hour_id


select happy_hours_count, count(1) from reports.tableau_raw_data_phase12
group by happy_hours_count

true	35321
false	17028496

select infogroup_id_chain_id, count(1) from reports.tableau_raw_data_phase12
group by infogroup_id_chain_id
1	1269787
0	15794030

1	1270425
0	15733722

*/


--select infogroup_id_ultimate_headquarters_id, count(1) from reports.tableau_raw_data_phase12
--group by infogroup_id_ultimate_headquarters_id

--select cnt_name, count(1) from reports.tableau_extract_phase12
--group by cnt_name

--select sum(cnt) from reports.tableau_extract_phase12

--This is how the analysis is run in tableau on the extract file and matches with it.
--select cnt_chain_id, sum(cnt), count(1) from reports.tableau_extract_phase12
--group by cnt_chain_id

--select Cnt_Growing_Business_Code, sum(cnt), count(1) from reports.tableau_extract_phase12
--group by Cnt_Growing_Business_Code

/*
it will be more than 1 count due to the combinations of groupings.

the resulting sum is the sum of indicators in a group:
so sum of Cnt_Growing_Business_Code, is the sum of 1's and 0's in a group,
a sum of 34 is when we sum up all of 1's and 0's in Cnt_Growing_Business_Code, the sum of the group is 34,
meaning 34 records within that group has an indicator of 1
*/

--select infogroup_id_adsize_code_tags, count(1) from reports.tableau_raw_data_phase12
--group by infogroup_id_adsize_code_tags


