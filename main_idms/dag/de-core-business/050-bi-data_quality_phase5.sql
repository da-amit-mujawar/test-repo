--select * from reports.professional_title_lk limit 10

DROP TABLE IF EXISTS reports.tableau_images_in_business;
-- create temp table with Intent  data for In_business='y'
SELECT m.infogroup_id,
       CASE WHEN (m.asset_url = '') THEN (NULL) ELSE (m.infogroup_id) END  infogroup_id_images_asset_url,
       CASE WHEN (m.asset_hash = '') THEN (NULL) ELSE (m.infogroup_id) END infogroup_id_images_asset_hash,
       CASE WHEN (m."primary" = 'true') THEN (m.infogroup_id) END          infogroup_id_images_primary
INTO reports.tableau_images_in_business
FROM core_bf.images m
         JOIN core_bf.places b ON b.infogroup_id = m.infogroup_id
WHERE in_business = 'yes';

--select * from reports.tableau_images_in_business limit 10

--summarized intent data at infogroup_id level
DROP TABLE IF EXISTS reports.tableau_images_data_final;
SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_images_asset_url)  infogroup_id_images_asset_url_img,
       COUNT(DISTINCT infogroup_id_images_asset_hash) infogroup_id_images_asset_hash_img,
       COUNT(DISTINCT infogroup_id_images_primary)    infogroup_id_images_primary_img
--count(distinct infogroup_id_naics_code_id) infogroup_id_naics_code_id_tags,
--count(distinct infogroup_id_yellow_page_code) infogroup_id_yellow_page_code_tags,
--count(distinct infogroup_id_yppa_code) infogroup_id_yppa_code_tags
INTO reports.tableau_images_data_final
FROM reports.tableau_images_in_business
GROUP BY infogroup_id;

DROP TABLE IF EXISTS reports.tableau_raw_data_phase5;
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
       b.infogroup_id,
       --Transformation of elements for phase 5
       ---bbb_business_review_url and single_platform_url not there on places.
       --CASE
       -- WHEN website_keywords = '' THEN 0
       --ELSE 1
       --END infogroup_id_website_keywords,
       -- CASE
       -- WHEN bbb_accredited = '' THEN 0
       -- ELSE 1
       --  END infogroup_id_bbb_accredited,
       --Categories: Business Profile
       CASE
           WHEN company_description = '' THEN 0
           ELSE 1
           END                      infogroup_id_company_description,
       CASE
           WHEN dress_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_dress_code,
       CASE
           WHEN equipment_rentals = '' THEN 0
           ELSE 1
           END                      infogroup_id_equipment_rentals,
       CASE
           WHEN has_ecommerce = '' THEN 0
           ELSE 1
           END                      infogroup_id_has_ecommerce,
       CASE
           WHEN languages_spoken = '' THEN 0
           ELSE 1
           END                      infogroup_id_languages_spoken,
       CASE
           WHEN price_range = '' THEN 0
           ELSE 1
           END                      infogroup_id_price_range,
       CASE
           WHEN professional_specialty_ids = '' THEN 0
           ELSE 1
           END                      infogroup_id_professional_specialty_ids,
       CASE
           WHEN public_access = '' THEN 0
           ELSE 1
           END                      infogroup_id_public_access,
       CASE
           WHEN shopping_center_atm = '' THEN 0
           ELSE 1
           END                      infogroup_id_shopping_center_atm,
       --  CASE
       --   WHEN cmra_flag = '' THEN 0 /* in phase 3*/
       -- ELSE 1
       --END infogroup_id_cmra_flag,
       CASE
           WHEN federal_government_contractor = '' THEN 0
           ELSE 1
           END                      infogroup_id_federal_government_contractor,
       --CASE
       -- WHEN dma_county_rank = '' THEN 0
       --ELSE 1
       --END infogroup_id_dma_county_rank
       --CASE
       -- WHEN dma_region = '' THEN 0
       --ELSE 1
       --END infogroup_id_dma_region
       -- No dma_territory too
       --Category-Payment method.

       CASE
           WHEN payment_types = '' THEN 0
           ELSE 1
           END                      infogroup_id_payment_types,
       CASE
           WHEN insurances_accepted = '' THEN 0
           ELSE 1
           END                      infogroup_id_insurances_accepted,
       CASE
           WHEN medicare_accepted = '' THEN 0
           ELSE 1
           END                      infogroup_id_medicare_accepted,
       CASE
           WHEN medicaid_accepted = '' THEN 0
           ELSE 1
           END                      infogroup_id_medicaid_accepted,

--need to add categories- images 4 elements
--Categories-Expenses.
       --for the expenses , it consists of ranges-hence there is a lower ranger a
       --and then an upper range. I have only utilized the upper range.

       CASE
           WHEN expenses_accounting_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_accounting_lower,
       CASE
           WHEN expenses_advertising_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_advertising_lower,
       CASE
           WHEN expenses_charities_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_charities_lower,

       CASE
           WHEN expenses_contract_labor_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_contract_labor_lower,
       CASE
           WHEN expenses_corporate = '' THEN 0
           ELSE 1
           END                      infogroup_id_expenses_corporate,
       CASE
           WHEN expenses_insurance_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_insurance_lower,

       CASE
           WHEN expenses_legal_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_legal_lower,
       CASE
           WHEN expenses_licenses_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_licenses_lower,
       CASE
           WHEN expenses_maintenance_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_maintenance_lower,


       CASE
           WHEN expenses_office_equipment_supplies_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_office_equipment_supplies_lower,
       CASE
           WHEN expenses_packaging_shipping_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_packaging_shipping_lower,
       CASE
           WHEN expenses_payroll_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_payroll_lower,
       CASE
           WHEN expenses_printing_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_printing_lower,
       CASE
           WHEN expenses_professional_services_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_professional_services_lower,
       CASE
           WHEN expenses_rent_lease_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_rent_lease_lower,
       CASE
           WHEN expenses_technology_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_technology_lower,
       CASE
           WHEN expenses_telecommunications_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_telecommunications_lower,
       CASE
           WHEN expenses_transportation_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_transportation_lower,
       CASE
           WHEN expenses_utilities_lower IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_expenses_utilities_lower,
--Categories- Geography
       CASE
           WHEN population_density IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_population_density,
       CASE
           WHEN population_code_for_zip = '' THEN 0
           ELSE 1
           END                      infogroup_id_population_code_for_zip,
       CASE
           WHEN wealthy_area_flag = '' THEN 0
           ELSE 1
           END                      infogroup_id_wealthy_area_flag,
--Add the images data transformation
       infogroup_id_images_asset_url_img,
       infogroup_id_images_asset_hash_img,
       infogroup_id_images_primary_img
INTO reports.tableau_raw_data_phase5
FROM core_bf.v_places_in_business b
         LEFT JOIN reports.professional_title_lk pt
                   ON (b.primary_contact_professional_title = pt.professional_title)
         LEFT JOIN reports.tableau_images_data_final m
                   ON (m.infogroup_id = b.infogroup_id)
WHERE in_business = 'yes';

--select * from core_bf.v_places_in_business    limit 10;


--select * from workspace.tableau_raw_data_phase5 limit 10;
--select distinct(sic_code_ids)from reports.tableau_raw_data_phase5
--select  infogroup_id, sic_code_ids from reports.tableau_raw_data_phase5
--group by sic_code_ids,infogroup_id

DROP TABLE IF EXISTS reports.tableau_extract_phase5;

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
       COUNT(infogroup_id)                                        cnt,
       --SUM(infogroup_id_website_keywords) cnt_website_keywords,
       SUM(infogroup_id_company_description)                      cnt_company_description,
       SUM(infogroup_id_dress_code)                               cnt_dress_code,
       SUM(infogroup_id_equipment_rentals)                        cnt_equipment_rentals,
       SUM(infogroup_id_has_ecommerce)                            cnt_has_ecommerce,
       SUM(infogroup_id_languages_spoken)                         cnt_languages_spoken,
       SUM(infogroup_id_price_range)                              cnt_price_range,
       SUM(infogroup_id_professional_specialty_ids)               cnt_professional_specialty_ids,
       SUM(infogroup_id_public_access)                            cnt_public_access,
       SUM(infogroup_id_shopping_center_atm)                      cnt_shopping_center_atm,
       --SUM(infogroup_id_cmra_flag) cnt_cmra_flag,
       SUM(infogroup_id_federal_government_contractor)            cnt_federal_government_contractor,
       SUM(infogroup_id_payment_types)                            cnt_payment_types,
       SUM(infogroup_id_insurances_accepted)                      cnt_insurances_accepted,
       SUM(infogroup_id_medicare_accepted)                        cnt_medicare_accepted,
       SUM(infogroup_id_medicaid_accepted)                        cnt_medicaid_accepted,
       --Need to add images
       SUM(infogroup_id_images_asset_url_img)                     cnt_images_asset_url_img,
       SUM(infogroup_id_images_asset_hash_img)                    cnt_images_asset_hash_img,
       SUM(infogroup_id_images_primary_img)                       cnt_images_primary_img,
       SUM(infogroup_id_expenses_accounting_lower)                cnt_expenses_accounting_lower,
       SUM(infogroup_id_expenses_advertising_lower)               cnt_expenses_advertising_lower,
       SUM(infogroup_id_expenses_charities_lower)                 cnt_expenses_charities_lower,
       SUM(infogroup_id_expenses_contract_labor_lower)            cnt_expenses_contract_labor_lower,
       SUM(infogroup_id_expenses_corporate)                       cnt_expenses_corporate,
       SUM(infogroup_id_expenses_insurance_lower)                 cnt_expenses_insurance_lower,
       SUM(infogroup_id_expenses_legal_lower)                     cnt_expenses_legal_lower,
       SUM(infogroup_id_expenses_licenses_lower)                  cnt_expenses_licenses_lower,
       SUM(infogroup_id_expenses_maintenance_lower)               cnt_expenses_maintenance_lower,
       SUM(infogroup_id_expenses_office_equipment_supplies_lower) cnt_expenses_office_equipment_supplies_lower,
       SUM(infogroup_id_expenses_packaging_shipping_lower)        cnt_expenses_packaging_shipping_lower,
       SUM(infogroup_id_expenses_payroll_lower)                   cnt_expenses_payroll_lower,
       SUM(infogroup_id_expenses_printing_lower)                  cnt_expenses_printing_lower,
       SUM(infogroup_id_expenses_professional_services_lower)     cnt_expenses_professional_services_lower,
       SUM(infogroup_id_expenses_rent_lease_lower)                cnt_expenses_rent_lease_lower,
       SUM(infogroup_id_expenses_technology_lower)                cnt_expenses_technology_lower,
       SUM(infogroup_id_expenses_telecommunications_lower)        cnt_expenses_telecommunications_lower,
       SUM(infogroup_id_expenses_transportation_lower)            cnt_expenses_transportation_lower,
       SUM(infogroup_id_expenses_utilities_lower)                 cnt_expenses_utilities_lower,
       SUM(infogroup_id_population_density)                       cnt_population_density,
       SUM(infogroup_id_population_code_for_zip)                  cnt_population_code_for_zip,
       SUM(infogroup_id_wealthy_area_flag)                        cnt_wealthy_area_flag
INTO reports.tableau_extract_phase5
FROM reports.tableau_raw_data_phase5
GROUP BY place_type, primary_sic_flg,
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
         logo_url_ind;

--select count(*) from reports.tableau_raw_data_phase5
--17004147
--select primary_contact, count(1) from reports.tableau_extract_phase5
--group by primary_contact

--true	9116130

--true	9109153


/*select  infogroup_id_images_primary_img, count(1) from reports.tableau_raw_data_phase5
group by infogroup_id_images_primary_img

1	1414961
	15589185
0	1
*/
--1	1416662

--select infogroup_id_professional_specialty_ids, count(1) from reports.tableau_raw_data_phase5
--group by infogroup_id_professional_specialty_ids
--1	1096440

--1	1090212

--select infogroup_id_payment_types, count(1) from reports.tableau_raw_data_phase5
--group by infogroup_id_payment_types

--1	3121131
--0	13942686

/*
select logo_url_ind, count(1),sum(cnt),
SUM(cnt_expenses_printing_lower) exp_print
       from reports.tableau_extract_phase5
      group by logo_url_ind;

select SUM(cnt_expenses_printing_lower) exp_print
from reports.tableau_extract_phase5
group by cnt_expenses_printing_lower;

*/
