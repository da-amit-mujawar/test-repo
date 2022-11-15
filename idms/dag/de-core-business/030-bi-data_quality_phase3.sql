--CREATE MATERIALIZED VIEW core_bf.v_places_in_business AS SELECT * FROM core_bf.places WHERE in_business = 'yes';

--CREATE  VIEW core_bf.v_places_in_business AS SELECT * FROM core_bf.places WHERE in_business = 'yes';
--DROP MATERIALIZED VIEW reports.v_places_in_business;
--CREATE table reports.v_places_in_business AS SELECT * FROM core_bf.places WHERE in_business = 'yes';
--Normally use the core_bf.v.places_in_business view if not available use the above.

--select count(*) from reports.v_places_in_business

DROP TABLE IF EXISTS reports.tableau_intents_in_business;
-- create temp table with Intent  data for In_business='y'
SELECT i.infogroup_id,
       CASE WHEN (i.id IS NULL) THEN (NULL) ELSE (i.infogroup_id) END     infogroup_id_intent_id,
       CASE WHEN (i.topic_ids = '') THEN (NULL) ELSE (i.infogroup_id) END infogroup_id_intent_topic_ids,
       CASE WHEN (i.score IS NULL) THEN (NULL) ELSE (i.infogroup_id) END  infogroup_id_intent_score
INTO reports.tableau_intents_in_business
FROM core_bf.intents i
         JOIN core_bf.places b ON b.infogroup_id = i.infogroup_id
WHERE in_business = 'yes';

--summarized intent data at infogroup_id level
DROP TABLE IF EXISTS reports.tableau_intents_data_final;
SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_intent_id)        infogroup_id_intent_id_intents,
       COUNT(DISTINCT infogroup_id_intent_topic_ids) infogroup_id_intent_topic_ids_intents,
       COUNT(DISTINCT infogroup_id_intent_score)     infogroup_id_intent_score_intents
--count(distinct infogroup_id_naics_code_id) infogroup_id_naics_code_id_tags,
--count(distinct infogroup_id_yellow_page_code) infogroup_id_yellow_page_code_tags,
--count(distinct infogroup_id_yppa_code) infogroup_id_yppa_code_tags
INTO reports.tableau_intents_data_final
FROM reports.tableau_intents_in_business
GROUP BY infogroup_id;

--select count(id) from core_bf.intents
--select count(infogroup_id_intent_id) from workspace.tableau_intents_in_business

--select count(infogroup_id_intent_topic_ids_intents) from workspace.tableau_intents_data_final group by
--infogroup_id_intent_topic_ids_intents;

DROP TABLE IF EXISTS reports.tableau_raw_data_phase3;
--Added the dimensions/filters in the beginning
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


       --RERUN this piece again
--Added for Phase 3 Hospital filter
       accepting_new_patients,
       hospital_has_emergency_room,
       -- CASE
       --     WHEN (accepting_new_patients ='') THEN ('FALSE')
       --   ELSE ('TRUE') end accepting_new_patients_ind,
       --CASE
       --   WHEN (hospital_has_emergency_room IS NOT NULL) THEN ('TRUE')
       -- ELSE ('false') end hospital_has_emergency_room_ind,

       b.infogroup_id,

--Transformation of elements for phase 3- Area & Postal
       CASE
           WHEN cbsa_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_cbsa_code,
       CASE
           WHEN cbsa_level = '' THEN 0
           ELSE 1
           END                      infogroup_id_cbsa_level,
       CASE
           WHEN csa_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_csa_code,
       CASE
           WHEN census_block = '' THEN 0
           ELSE 1
           END                      infogroup_id_census_block,
       CASE
           WHEN census_tract = '' THEN 0
           ELSE 1
           END                      infogroup_id_census_tract,
       CASE
           WHEN carrier_route_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_carrier_route_code,
       CASE
           WHEN mailing_address_carrier_route_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_address_carrier_route_code,
       CASE
           WHEN fips_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_fips_code,
       CASE
           WHEN sgc_division = '' THEN 0
           ELSE 1
           END                      infogroup_id_sgc_division,
       CASE
           WHEN mailing_score_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_score_code,
       CASE
           WHEN mailing_address_score_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_address_score_code,
       CASE
           WHEN cmra_flag = '' THEN 0
           ELSE 1
           END                      infogroup_id_cmra_flag,
       CASE
           WHEN mailing_address_cmra_flag = '' THEN 0
           ELSE 1
           END                      infogroup_id_mailing_address_cmra_flag,
       --CASE
       -- WHEN dma_county_rank = '' THEN 0
       --ELSE 1
       --END infogroup_id_dma_county_rank
       --CASE
       -- WHEN dma_region = '' THEN 0
       --ELSE 1
       --END infogroup_id_dma_region
       -- No dma_territory too

--Phase 3- category-Parking begins here
       CASE
           WHEN parking_bike_rack = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_bike_rack,
       CASE
           WHEN parking_bike_lockers = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_bike_lockers,

       CASE
           WHEN parking_electric_charging_port_count IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_parking_electric_charging_port_count,
       CASE
           WHEN parking_free = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_free,
       CASE
           WHEN parking_number_of_spaces IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_parking_number_of_spaces,
       CASE
           WHEN parking_onsite = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_onsite,
       CASE
           WHEN parking_overnight_parking = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_overnight_parking,
       CASE
           WHEN parking_permit_required = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_permit_required,
       CASE
           WHEN parking_public = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_public,
       CASE
           WHEN parking_transit_lines = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_transit_lines,
       CASE
           WHEN parking_valet = '' THEN 0
           ELSE 1
           END                      infogroup_id_parking_valet,

       --Category-Incorporation
       CASE
           WHEN nbrc_corporation_filing_type = '' THEN 0
           ELSE 1
           END                      infogroup_id_nbrc_corporation_filing_type,
       CASE
           WHEN nbrc_corporate_date = '' THEN 0
           ELSE 1
           END                      infogroup_id_nbrc_corporate_date,
       -- Intent data added above

       --Phase 3, category-medical
       CASE
           WHEN hospital_has_emergency_room = '' THEN 0
           ELSE 1
           END                      infogroup_id_hospital_has_emergency_room,
       CASE
           WHEN ami_physician_primary_specialty_code IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_ami_physician_primary_specialty_code,

       CASE
           WHEN ami_physician_secondary_specialty_code IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_ami_physician_secondary_specialty_code,
       CASE
           WHEN ami_board_certified_flag = '' THEN 0
           ELSE 1
           END                      infogroup_id_ami_board_certified_flag,

       CASE
           WHEN physician_year_of_graduation IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_physician_year_of_graduation,
       CASE
           WHEN ami_medical_school_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_ami_medical_school_code,

       CASE
           WHEN ami_hospital_number = '' THEN 0
           ELSE 1
           END                      infogroup_id_ami_hospital_number,

       CASE
           WHEN upin = '' THEN 0
           ELSE 1
           END                      infogroup_id_upin,
       CASE
           WHEN ami_license_number = '' THEN 0
           ELSE 1
           END                      infogroup_id_ami_license_number,
       CASE
           WHEN ami_state_of_license = '' THEN 0
           ELSE 1
           END                      infogroup_id_ami_state_of_license,
       CASE
           WHEN physician_year_of_birth IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_physician_year_of_birth,
       CASE
           WHEN ami_residency_hospital_code = '' THEN 0
           ELSE 1
           END                      infogroup_id_ami_residency_hospital_code,
       CASE
           WHEN residency_graduation_year IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_residency_graduation_year,
       CASE
           WHEN ami_national_provider_flag = '' THEN 0
           ELSE 1
           END                      infogroup_id_ami_national_provider_flag,
       CASE
           WHEN medical_dea_number = '' THEN 0
           ELSE 1
           END                      infogroup_id_medical_dea_number,

       CASE
           WHEN accepting_new_patients = '' THEN 0
           ELSE 1
           END                      infogroup_id_accepting_new_patients,

--Intent data category

       infogroup_id_intent_id_intents,
       infogroup_id_intent_topic_ids_intents,
       infogroup_id_intent_score_intents

INTO reports.tableau_raw_data_phase3
FROM core_bf.v_places_in_business b
         --FROM reports.mv_places_in_business b
         LEFT JOIN reports.professional_title_lk pt
                   ON (b.primary_contact_professional_title = pt.professional_title)
         LEFT JOIN reports.tableau_intents_data_final i
                   ON (i.infogroup_id = b.infogroup_id)
WHERE in_business = 'yes';

DROP TABLE IF EXISTS reports.tableau_extract_phase3;
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
       accepting_new_patients,
       hospital_has_emergency_room,

       COUNT(infogroup_id)                                      cnt,
       SUM(infogroup_id_cbsa_code)                              cnt_cbsa_code,
       SUM(infogroup_id_cbsa_level)                             cnt_cbsa_level,
       SUM(infogroup_id_csa_code)                               cnt_csa_code,
       SUM(infogroup_id_census_block)                           cnt_census_block,
       SUM(infogroup_id_census_tract)                           cnt_census_tract,
       SUM(infogroup_id_carrier_route_code)                     cnt_carrier_route_code,
       SUM(infogroup_id_mailing_address_carrier_route_code)     cnt_mailing_address_carrier_route_code,
       SUM(infogroup_id_fips_code)                              cnt_fips_code,
       SUM(infogroup_id_sgc_division)                           cnt_sgc_division,
       SUM(infogroup_id_mailing_score_code)                     cnt_mailing_score_code,
       SUM(infogroup_id_mailing_address_score_code)             cnt_mailing_address_score_code,
       SUM(infogroup_id_cmra_flag)                              cnt_cmra_flag,
       SUM(infogroup_id_mailing_address_cmra_flag)              cnt_mailing_address_cmra_flag,
       --dma not there 3 vars
       SUM(infogroup_id_parking_bike_rack)                      cnt_parking_bike_rack,
       SUM(infogroup_id_parking_bike_lockers)                   cnt_parking_bike_lockers,
       SUM(infogroup_id_parking_electric_charging_port_count)   cnt_parking_electric_charging_port_count,
       SUM(infogroup_id_parking_free)                           cnt_parking_free,
       SUM(infogroup_id_parking_number_of_spaces)               cnt_parking_number_of_spaces,
       SUM(infogroup_id_parking_onsite)                         cnt_parking_onsite,
       SUM(infogroup_id_parking_overnight_parking)              cnt_parking_overnight_parking,
       SUM(infogroup_id_parking_permit_required)                cnt_parking_permit_required,
       SUM(infogroup_id_parking_public)                         cnt_parking_public,
       SUM(infogroup_id_parking_transit_lines)                  cnt_parking_transit_lines,
       SUM(infogroup_id_parking_valet)                          cnt_parking_valet,
       SUM(infogroup_id_nbrc_corporation_filing_type)           cnt_nbrc_corporation_filing_type,
       SUM(infogroup_id_nbrc_corporate_date)                    cnt_nbrc_corporate_date,
       SUM(infogroup_id_intent_id_intents)                      cnt_id_intents,
       SUM(infogroup_id_intent_topic_ids_intents)               cnt_topic_ids_intents,
       SUM(infogroup_id_intent_score_intents)                   cnt_score_intents,
       SUM(infogroup_id_hospital_has_emergency_room)            cnt_hospital_has_emergency_room,
       SUM(infogroup_id_ami_physician_primary_specialty_code)   cnt_ami_physician_primary_specialty_code,
       SUM(infogroup_id_ami_physician_secondary_specialty_code) cnt_ami_physician_secondary_specialty_code,
       SUM(infogroup_id_ami_board_certified_flag)               cnt_ami_board_certified_flag,
       SUM(infogroup_id_physician_year_of_graduation)           cnt_physician_year_of_graduation,
       SUM(infogroup_id_ami_medical_school_code)                cnt_ami_medical_school_code,
       SUM(infogroup_id_ami_hospital_number)                    cnt_ami_hospital_number,
       SUM(infogroup_id_upin)                                   cnt_upin,
       SUM(infogroup_id_ami_license_number)                     cnt_ami_license_number,
       SUM(infogroup_id_ami_state_of_license)                   cnt_ami_state_of_license,
       SUM(infogroup_id_physician_year_of_birth)                cnt_physician_year_of_birth,
       SUM(infogroup_id_ami_residency_hospital_code)            cnt_ami_residency_hospital_code,
       SUM(infogroup_id_residency_graduation_year)              cnt_residency_graduation_year,
       SUM(infogroup_id_ami_national_provider_flag)             cnt_ami_national_provider_flag,
       SUM(infogroup_id_medical_dea_number)                     cnt_medical_dea_number,
       SUM(infogroup_id_accepting_new_patients)                 cnt_accepting_new_patients
INTO reports.tableau_extract_phase3
FROM reports.tableau_raw_data_phase3
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
         accepting_new_patients,
         hospital_has_emergency_room;

/*
select count(1),sum(cnt),
       SUM(cnt_id_intents)cnt_id,
       SUM(cnt_topic_ids_intents) cnt_topics_ids_intents,
       SUM(cnt_score_intents) cnt_score_intents
       --SUM(cnt_id_yppa_code_tags)
       from reports.tableau_extract_phase3;
*/

--select infogroup_id_medical_dea_number, count(1) from reports.tableau_raw_data_phase3
--group by infogroup_id_medical_dea_number

--select infogroup_id_intent_id_intents, count(1) from reports.tableau_raw_data_phase3
--group by infogroup_id_intent_id_intents

--1	4970895
--1	485188

--select accepting_new_patients, count(1) from reports.tableau_raw_data_phase3
--group by accepting_new_patients

--16407231
--false	42195
--true	534149

--	16433474
--false	41700
--true	528973

--select place_type from core_bf.places limit 10

/*
Per tableau extract file:
select cnt_cbsa_code, sum(cnt), count(1) from reports.tableau_extract_phase3
group by cnt_cbsa_code


select cnt_physician_year_of_birth, sum(cnt), count(1) from reports.tableau_extract_phase3
group by cnt_physician_year_of_birth

select cnt_cbsa_code, sum(cnt), count(1) from reports.tableau_extract_phase3
group by cnt_cbsa_code

select cnt_parking_number_of_spaces, sum(cnt), count(1) from reports.tableau_extract_phase3
group by cnt_parking_number_of_spaces



select infogroup_id_cbsa_code, count(1) from reports.tableau_raw_data_phase3
group by infogroup_id_cbsa_code

select infogroup_id_physician_year_of_birth, count(1) from reports.tableau_raw_data_phase3
group by infogroup_id_physician_year_of_birth

0	16499204
1	504943

*/

/*select count(*) from reports.tableau_raw_data_phase3
17004147*/
