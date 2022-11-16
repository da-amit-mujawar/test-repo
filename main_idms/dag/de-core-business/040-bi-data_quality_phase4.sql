--I have created the tables in two ways.
-- create temp table with Contact data for In_business='y'
/*In phase 4-contacts are removed as filters. */

--The below two codes are when the places (child) file is matched to the contacts file (master). Hence here
--when contact id is present and in_business =yes=73 million is the count and in_business on the contact = 73 Million.

DROP TABLE IF EXISTS reports.tableau_contacts_in_business;
CREATE TABLE reports.tableau_contacts_in_business AS
SELECT c.*, b.in_business
FROM core_bf.contacts c
         LEFT JOIN CORE_BF.PLACES b ON c.infogroup_id = b.infogroup_id
WHERE in_business = 'yes';

DROP TABLE IF EXISTS reports.tableau_contacts_in_business1;
CREATE TABLE reports.tableau_contacts_in_business1 AS
SELECT c.*, b.in_business
FROM core_bf.contacts c
         LEFT JOIN core_bf.v_places_in_business b ON c.infogroup_id = b.infogroup_id
WHERE in_business = 'yes';

--In here the Places as the master file and contacts as the child file (USE  BELOW)
--select count(id) from core_bf.contacts
--Catergory-contacts
DROP TABLE IF EXISTS reports.contacts_in_business2;
-- create temp table with Tags data for In_business='y'
SELECT c.infogroup_id,
       CASE WHEN (c.id = '') THEN (NULL) ELSE (c.infogroup_id) END            infogroup_id_contacts_id,
       CASE WHEN (c.email = '') THEN (NULL) ELSE (c.infogroup_id) END         infogroup_id_contacts_email,
       CASE WHEN (c.email_md5 = '') THEN (NULL) ELSE (c.infogroup_id) END     infogroup_id_contacts_email_md5,
       CASE WHEN (c.email_sha256 = '') THEN (NULL) ELSE (c.infogroup_id) END  infogroup_id_contacts_email_sha256,
       CASE
           WHEN (c.email_vendor_id = '') THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_email_vendor_id,
       CASE
           WHEN (c.email_deliverable = '') THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_email_deliverable,
       CASE
           WHEN (c.email_marketable = '') THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_email_marketable,
       CASE
           WHEN (c.email_reputation_risk = '') THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_email_reputation_risk,
       CASE WHEN (c.first_name = '') THEN (NULL) ELSE (c.infogroup_id) END    infogroup_id_contacts_first_name,
       CASE WHEN (c.gender = '') THEN (NULL) ELSE (c.infogroup_id) END        infogroup_id_contacts_gender,
       CASE
           WHEN (c.job_function_id IS NULL) THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_job_function_id,
       CASE WHEN (c.job_titles = '') THEN (NULL) ELSE (c.infogroup_id) END    infogroup_id_contacts_job_titles,
       CASE WHEN (c.job_title_ids = '') THEN (NULL) ELSE (c.infogroup_id) END infogroup_id_contacts_job_title_ids,
       CASE WHEN (c.last_name = '') THEN (NULL) ELSE (c.infogroup_id) END     infogroup_id_contacts_last_name,
       CASE
           WHEN (c.management_level = '') THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_management_level,
       CASE
           WHEN (c.mapped_contact_id = '') THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_mapped_contact_id,
       CASE
           WHEN (c.professional_title = '') THEN (NULL)
           ELSE (c.infogroup_id) END                                          infogroup_id_contacts_professional_title,
       CASE WHEN (c.primary = '') THEN (NULL) ELSE (c.infogroup_id) END       infogroup_id_contacts_primary,
       CASE WHEN (c.title_codes = '') THEN (NULL) ELSE (c.infogroup_id) END   infogroup_id_contacts_title_codes,
       CASE WHEN (c.vendor_id = '') THEN (NULL) ELSE (c.infogroup_id) END     infogroup_id_contacts_vendor_id
--case when (c.ownership_changed_on='') then (NULL) else (c.infogroup_id) end infogroup_id_contacts_ownership_changed_on
--case when (c.title_codes='') then (NULL) else (c.infogroup_id) end infogroup_id_contacts_title_codes
INTO reports.contacts_in_business2
FROM core_bf.contacts c
--here I have used the verified file to reduce the time
         JOIN core_bf.v_places_in_business b ON b.infogroup_id = c.infogroup_id
WHERE in_business = 'yes';


--select infogroup_id_contacts_professional_title, count(1) from reports.contacts_in_business2
--group by infogroup_id_contacts_professional_title


--summarized contact data at infogroup_id level
DROP TABLE IF EXISTS reports.tableau_contacts_data_final;

SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_contacts_id)                    infogroup_id_contacts_id_con,
       COUNT(DISTINCT infogroup_id_contacts_email)                 infogroup_id_contacts_email_con,
       COUNT(DISTINCT infogroup_id_contacts_email_md5)             infogroup_id_contacts_email_md5_con,
       COUNT(DISTINCT infogroup_id_contacts_email_sha256)          infogroup_id_contacts_email_sha256_con,
       COUNT(DISTINCT infogroup_id_contacts_email_vendor_id)       infogroup_id_contacts_email_vendor_id_con,
       COUNT(DISTINCT infogroup_id_contacts_email_deliverable)     infogroup_id_contacts_email_deliverable_con,
       COUNT(DISTINCT infogroup_id_contacts_email_marketable)      infogroup_id_contacts_email_marketable_con,
       COUNT(DISTINCT infogroup_id_contacts_email_reputation_risk) infogroup_id_contacts_email_reputation_risk_con,
       COUNT(DISTINCT infogroup_id_contacts_first_name)            infogroup_id_contacts_first_name_con,
       COUNT(DISTINCT infogroup_id_contacts_gender)                infogroup_id_contacts_gender_con,
       COUNT(DISTINCT infogroup_id_contacts_job_function_id)       infogroup_id_contacts_job_function_id_con,
       COUNT(DISTINCT infogroup_id_contacts_job_titles)            infogroup_id_contacts_job_titles_con,
       COUNT(DISTINCT infogroup_id_contacts_job_title_ids)         infogroup_id_contacts_job_title_ids_con,
       COUNT(DISTINCT infogroup_id_contacts_last_name)             infogroup_id_contacts_last_name_con,
       COUNT(DISTINCT infogroup_id_contacts_management_level)      infogroup_id_contacts_management_level_con,
       COUNT(DISTINCT infogroup_id_contacts_mapped_contact_id)     infogroup_id_contacts_mapped_contact_id_con,
       COUNT(DISTINCT infogroup_id_contacts_professional_title)    infogroup_id_contacts_professional_title_con,
       COUNT(DISTINCT infogroup_id_contacts_primary)               infogroup_id_contacts_primary_con,
       COUNT(DISTINCT infogroup_id_contacts_title_codes)           infogroup_id_contacts_title_codes_con,
       COUNT(DISTINCT infogroup_id_contacts_vendor_id)             infogroup_id_contacts_vendor_id_con
INTO reports.tableau_contacts_data_final
FROM reports.contacts_in_business2
GROUP BY infogroup_id;


/*
select infogroup_id_contacts_id_con, count(1) from reports.tableau_contacts_data_final
group by infogroup_id_contacts_id_con

select infogroup_id_contacts_management_level_con, count(1) from reports.tableau_contacts_data_final
group by infogroup_id_contacts_management_level_con

1	8955016

select count(*) from reports.tableau_contacts_in_business2 ;

select count(email) from reports.tableau_contacts_in_business1;*/

--RUN FROM HERE


DROP TABLE IF EXISTS reports.tableau_raw_data_phase4;
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
           WHEN country_code = 'US' THEN SUBSTRING(postal_code, 1, 5)
           WHEN country_code = 'CA' THEN SUBSTRING(postal_code, 1, 6)
           END postal_code,
       state,
       city,
       -- Use the primary_contact_primary
       --CASE
       -- WHEN primary_contact_id = '' THEN ('false')
       --ELSE ('true')
       --END primary_contact,
       --CASE
       --WHEN primary_contact_primary = '' THEN ('false')
       --ELSE ('true')
       --END primary_contact,


       --pt.labels_professional_title primary_contact_professional_title,
       --primary_contact_title_codes,
       --primary_contact_management_level,
       --CASE
       --WHEN (primary_contact_email = '') THEN ('false')
       --ELSE ('true')
       --END primary_contact_email,
       -- primary_contact_email_deliverable,
       --primary_contact_email_marketable,
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
           END corporate_sales_volume,
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
           END location_sales_volume,
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
           WHEN (estimated_location_employee_count >= 1000 AND estimated_location_employee_count <= 4999)
               THEN ('1,000-4,999')
           WHEN (estimated_location_employee_count >= 5000 AND estimated_location_employee_count <= 9999)
               THEN ('5,000-9,999')
           WHEN (estimated_location_employee_count >= 10000) THEN ('10,000+')
           END estimated_location_employee_size,
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
       infogroup_id_contacts_id_con,
       CASE
           WHEN (infogroup_id_contacts_email_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_email_con1,

       CASE
           WHEN (infogroup_id_contacts_email_md5_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_email_md5_con1,


       CASE
           WHEN (infogroup_id_contacts_email_sha256_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_email_sha256_con1,

       CASE
           WHEN (infogroup_id_contacts_email_vendor_id_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_email_vendor_id_con1,


       CASE
           WHEN (infogroup_id_contacts_email_deliverable_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_email_deliverable_con1,

       CASE
           WHEN (infogroup_id_contacts_email_marketable_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_email_marketable_con1,
       CASE
           WHEN (infogroup_id_contacts_email_reputation_risk_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_email_reputation_risk_con1,
       CASE
           WHEN (infogroup_id_contacts_first_name_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_first_name_con1,
       CASE
           WHEN (infogroup_id_contacts_gender_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_gender_con1,

       CASE
           WHEN (infogroup_id_contacts_job_function_id_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_job_function_id_con1,


       CASE
           WHEN (infogroup_id_contacts_job_titles_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_job_titles_con1,

       CASE
           WHEN (infogroup_id_contacts_job_title_ids_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_job_title_ids_con1,
       CASE
           WHEN (infogroup_id_contacts_last_name_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_last_name_con1,

       CASE
           WHEN (infogroup_id_contacts_management_level_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_management_level_con1,

       CASE
           WHEN (infogroup_id_contacts_mapped_contact_id_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_mapped_contact_id_con1,

       CASE
           WHEN (infogroup_id_contacts_professional_title_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_professional_title_con1,

       CASE
           WHEN (infogroup_id_contacts_primary_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_primary_con1,

       CASE
           WHEN (infogroup_id_contacts_title_codes_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_title_codes_con1,
       CASE
           WHEN (infogroup_id_contacts_vendor_id_con = 1) THEN 1
           ELSE 0
           END infogroup_id_contacts_vendor_id_con1


--case when (place_type='headquarters') then (1) else (0)
--end infogroup_id_headquarters_id,

       --infogroup_id_contacts_email_con,
       -- infogroup_id_contacts_email_md5_con,
       --infogroup_id_contacts_email_sha256_con,
       --infogroup_id_contacts_email_vendor_id_con,
       --infogroup_id_contacts_email_deliverable_con,
       --infogroup_id_contacts_email_marketable_con,
       --infogroup_id_contacts_email_reputation_risk_con,
       -- infogroup_id_contacts_first_name_con,
--  infogroup_id_contacts_gender_con,
       --infogroup_id_contacts_job_function_id_con,
       --infogroup_id_contacts_job_titles_con,
       --infogroup_id_contacts_job_title_ids_con,
       --infogroup_id_contacts_last_name_con,
       --infogroup_id_contacts_management_level_con,
       --infogroup_id_contacts_mapped_contact_id_con,
       --infogroup_id_contacts_professional_title_con,
       --infogroup_id_contacts_primary_con,
       --infogroup_id_contacts_title_codes_con,
       --infogroup_id_contacts_vendor_id_con

INTO reports.tableau_raw_data_phase4
FROM core_bf.v_places_in_business b
--FROM reports.mv_places_in_business b
         LEFT JOIN reports.tableau_contacts_data_final c
                   ON (c.infogroup_id = b.infogroup_id)
WHERE in_business = 'yes';

DROP TABLE IF EXISTS reports.tableau_extract_phase4;

SELECT place_type,
       primary_sic_flg,
       primary_sic_code_id,
       sic_code_ids,
       country_code,
       postal_code,
       state,
       city,
    /*   primary_contact,
       primary_contact_professional_title,
       primary_contact_title_codes,
       primary_contact_management_level,
       primary_contact_email,
       primary_contact_email_deliverable,
       primary_contact_email_marketable,*/
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
    /* restaurant_cuisines,
     restaurant_limited_service,
     restaurant_reservations,
     restaurant_takeout,
     happy_hours_count,
     hotel_id,
     hotel_continental_breakfast,
     hotel_elevator,
     hotel_exercise_facility,
     hotel_guest_laundry,
     hotel_hot_tub,
     hotel_indoor_pool,
     hotel_kitchens,
     hotel_outdoor_pool,
     hotel_pet_friendly,
     hotel_room_service,*/


       COUNT(infogroup_id)                                   cnt,
       SUM(infogroup_id_contacts_id_con)                     cnt_contacts_id_con,
       SUM(infogroup_id_contacts_email_con1)                 cnt_contacts_email_con,
       SUM(infogroup_id_contacts_email_md5_con1)             cnt_contacts_email_md5_con,
       SUM(infogroup_id_contacts_email_sha256_con1)          cnt_contacts_email_sha256_con,
       SUM(infogroup_id_contacts_email_vendor_id_con1)       cnt_contacts_email_vendor_id_con,
       SUM(infogroup_id_contacts_email_deliverable_con1)     cnt_contacts_email_deliverable_con,
       SUM(infogroup_id_contacts_email_marketable_con1)      cnt_contacts_email_marketable_con,
       SUM(infogroup_id_contacts_email_reputation_risk_con1) cnt_contacts_email_reputation_risk_con,
       SUM(infogroup_id_contacts_first_name_con1)            cnt_contacts_first_name_con,
       SUM(infogroup_id_contacts_gender_con1)                cnt_contacts_gender_con,
       SUM(infogroup_id_contacts_job_function_id_con1)       cnt_contacts_job_function_id_con,
       SUM(infogroup_id_contacts_job_titles_con1)            cnt_contacts_job_titles_con,
       SUM(infogroup_id_contacts_job_title_ids_con1)         cnt_contacts_job_title_ids_con,
       SUM(infogroup_id_contacts_last_name_con1)             cnt_contacts_last_name_con,
       SUM(infogroup_id_contacts_management_level_con1)      cnt_contacts_management_level_con,
       SUM(infogroup_id_contacts_mapped_contact_id_con1)     cnt_contacts_mapped_contact_id_con,
       SUM(infogroup_id_contacts_professional_title_con1)    cnt_contacts_professional_title_con,
       SUM(infogroup_id_contacts_primary_con1)               cnt_contacts_primary_con,
       SUM(infogroup_id_contacts_title_codes_con1)           cnt_contacts_title_codes_con,
       SUM(infogroup_id_contacts_vendor_id_con1)             cnt_contacts_vendor_id_con


INTO reports.tableau_extract_phase4

FROM reports.tableau_raw_data_phase4
GROUP BY place_type, primary_sic_flg,
         primary_sic_code_id,
         sic_code_ids,
         country_code,
         postal_code,
         state,
         city,
         -- primary_contact,
         --primary_contact_professional_title,
         --primary_contact_title_codes,
         --primary_contact_management_level,
         --primary_contact_email,
         --primary_contact_email_deliverable,
         --primary_contact_email_marketable,
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
         logo_url_ind;

/*

select primary_contact_professional_title, count(1),sum(cnt),
SUM(cnt_adsize_code_tags),
       SUM(cnt_primary_tags) cnt_primary_tags,
      SUM(cnt_sic_code_id_tags) cnt_sic_code_id,
       SUM(cnt_naics_code_id_tags) cnt_naics_code_id,
       SUM(cnt_yellow_page_code_tags) cnt_yellow_page_code,
       SUM(cnt_id_yppa_code_tags) from workspace.tableau_extract_phase12
       group by primary_contact_professional_title;

-- 13,120,951
-- 13,351,657



select primary_contact, count(1) from reports.tableau_extract_phase12
group by primary_contact

--true	9116130

true	9118513

select restaurant_cuisines, count(1) from reports.tableau_extract_phase12
group by restaurant_cuisines

--True	526809
True	526503

select  infogroup_id_contacts_id_con, count(1) from reports.tableau_raw_data_phase4
group by  infogroup_id_contacts_id_con

select  infogroup_id_contacts_last_name_con1, count(1) from reports.tableau_raw_data_phase4
group by infogroup_id_contacts_last_name_con1

1	12802979
1	12814556

select infogroup_id_contacts_email_md5_con1, count(1) from reports.tableau_raw_data_phase4
group by infogroup_id_contacts_email_md5_con1

select infogroup_id_contacts_vendor_id_con1, count(1) from reports.tableau_raw_data_phase4
group by infogroup_id_contacts_vendor_id_con1

1	4913755
*/

--select count(*) from reports.tableau_raw_data_phase4
--17004147

/*

select count(*) from reports.tableau_contacts_in_business1 ;

select count(email) from reports.tableau_contacts_in_business1;
*/
