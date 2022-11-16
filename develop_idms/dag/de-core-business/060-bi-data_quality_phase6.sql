--DROP TABLE reports.professional_title_lk;
--CREATE TABLE reports.professional_title_lk as select distinct professional_title, labels_professional_title from core_bf.contacts;

--select count(*) from reports.mv_places_in_business
--select professional_title, labels_professional_title from reports.professional_title_lk


--select count(*) from core_bf.ucc_filings
--joining UCC tables
DROP TABLE IF EXISTS reports.tableau_ucc_filings_in_business;
-- create temp table with Intent  data for In_business='y'
SELECT u.infogroup_id,
       CASE
           WHEN (u.internal_record_key = '') THEN (NULL)
           ELSE (u.infogroup_id) END                                               infogroup_id_ucc_internal_record_key,
       CASE WHEN (u.amendment_id = '') THEN (NULL) ELSE (u.infogroup_id) END       infogroup_id_ucc_amendment_id,
       CASE WHEN (u.collateral_types = '') THEN (NULL) ELSE (u.infogroup_id) END   infogroup_id_ucc_collateral_types,
       CASE WHEN (u.expiration_date = '') THEN (NULL) ELSE (u.infogroup_id) END    infogroup_id_ucc_expiration_date,
       CASE WHEN (u.filing_date = '') THEN (NULL) ELSE (u.infogroup_id) END        infogroup_id_ucc_filing_date,
       CASE WHEN (u.filing_type = '') THEN (NULL) ELSE (u.infogroup_id) END        infogroup_id_ucc_filing_type,
       CASE WHEN (u.jurisdiction = '') THEN (NULL) ELSE (u.infogroup_id) END       infogroup_id_ucc_jurisdiction,
       CASE WHEN (u.secured_party_city = '') THEN (NULL) ELSE (u.infogroup_id) END infogroup_id_ucc_secured_party_city,
       CASE WHEN (u.secured_party_key = '') THEN (NULL) ELSE (u.infogroup_id) END  infogroup_id_ucc_secured_party_key,
       CASE WHEN (u.secured_party_name = '') THEN (NULL) ELSE (u.infogroup_id) END infogroup_id_ucc_secured_party_name,
       CASE
           WHEN (u.secured_party_postal_code = '') THEN (NULL)
           ELSE (u.infogroup_id) END                                               infogroup_id_ucc_secured_party_postal_code,
       CASE
           WHEN (u.secured_party_state = '') THEN (NULL)
           ELSE (u.infogroup_id) END                                               infogroup_id_ucc_secured_party_state,
       CASE
           WHEN (u.secured_party_street = '') THEN (NULL)
           ELSE (u.infogroup_id) END                                               infogroup_id_ucc_secured_party_street,
       CASE WHEN (u.status = '') THEN (NULL) ELSE (u.infogroup_id) END             infogroup_id_ucc_status

INTO reports.tableau_ucc_filings_in_business
FROM core_bf.ucc_filings u
         JOIN core_bf.places b ON b.infogroup_id = u.infogroup_id
WHERE in_business = 'yes';

--select * from reports.tableau_images_in_business limit 10

--summarized intent data at infogroup_id level
DROP TABLE IF EXISTS reports.tableau_ucc_filings_data_final;

SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_ucc_internal_record_key)       infogroup_id_ucc_internal_record_key_ucc,
       COUNT(DISTINCT infogroup_id_ucc_amendment_id)              infogroup_id_ucc_amendment_id_ucc,
       COUNT(DISTINCT infogroup_id_ucc_collateral_types)          infogroup_id_ucc_collateral_types_ucc,
       COUNT(DISTINCT infogroup_id_ucc_expiration_date)           infogroup_id_ucc_expiration_date_ucc,
       COUNT(DISTINCT infogroup_id_ucc_filing_date)               infogroup_id_ucc_filing_date_ucc,
       COUNT(DISTINCT infogroup_id_ucc_filing_type)               infogroup_id_ucc_filing_type_ucc,
       COUNT(DISTINCT infogroup_id_ucc_jurisdiction)              infogroup_id_ucc_jurisdiction_ucc,
       COUNT(DISTINCT infogroup_id_ucc_secured_party_city)        infogroup_id_ucc_secured_party_city_ucc,
       COUNT(DISTINCT infogroup_id_ucc_secured_party_key)         infogroup_id_ucc_secured_party_key_ucc,
       COUNT(DISTINCT infogroup_id_ucc_secured_party_name)        infogroup_id_ucc_secured_party_name_ucc,
       COUNT(DISTINCT infogroup_id_ucc_secured_party_postal_code) infogroup_id_ucc_secured_party_postal_code_ucc,
       COUNT(DISTINCT infogroup_id_ucc_secured_party_state)       infogroup_id_ucc_secured_party_state_ucc,
       COUNT(DISTINCT infogroup_id_ucc_secured_party_street)      infogroup_id_ucc_secured_party_street_ucc,
       COUNT(DISTINCT infogroup_id_ucc_status)                    infogroup_id_ucc_status_ucc
INTO reports.tableau_ucc_filings_data_final
FROM reports.tableau_ucc_filings_in_business
GROUP BY infogroup_id;

DROP TABLE IF EXISTS reports.tableau_benefits_plans_in_business;

SELECT e.infogroup_id,
       CASE WHEN (e.ack_id = '') THEN (NULL) ELSE (e.infogroup_id) END            infogroup_id_benefits_ack_id,
       CASE
           WHEN (e.active_beginning_participants IS NULL) THEN (NULL)
           ELSE (e.infogroup_id) END                                              infogroup_id_benefits_active_beginning_participants,
       CASE
           WHEN (e.active_ending_participants IS NULL) THEN (NULL)
           ELSE (e.infogroup_id) END                                              infogroup_id_benefits_active_ending_participants,
       CASE
           WHEN (e.benefit_arrangements = '') THEN (NULL)
           ELSE (e.infogroup_id) END                                              infogroup_id_benefits_benefit_arrangements,
       CASE WHEN (e.broker_city = '') THEN (NULL) ELSE (e.infogroup_id) END       infogroup_id_benefits_broker_city,
       CASE WHEN (e.broker_name = '') THEN (NULL) ELSE (e.infogroup_id) END       infogroup_id_benefits_broker_name,
       CASE
           WHEN (e.broker_postal_code = '') THEN (NULL)
           ELSE (e.infogroup_id) END                                              infogroup_id_benefits_broker_postal_code,
       CASE WHEN (e.broker_state = '') THEN (NULL) ELSE (e.infogroup_id) END      infogroup_id_benefits_broker_state,
       CASE WHEN (e.broker_street = '') THEN (NULL) ELSE (e.infogroup_id) END     infogroup_id_benefits_broker_street,
       CASE WHEN (e.carrier = '') THEN (NULL) ELSE (e.infogroup_id) END           infogroup_id_benefits_carrier,
       CASE WHEN (e.contract_number = '') THEN (NULL) ELSE (e.infogroup_id) END   infogroup_id_benefits_contract_number,
       CASE WHEN (e.effective_date IS NULL) THEN (NULL) ELSE (e.infogroup_id) END infogroup_id_benefits_effective_date,
       CASE WHEN (e.form_id IS NULL) THEN (NULL) ELSE (e.infogroup_id) END        infogroup_id_benefits_form_id,
       CASE
           WHEN (e.funding_arrangements = '') THEN (NULL)
           ELSE (e.infogroup_id) END                                              infogroup_id_benefits_funding_arrangements,
       CASE WHEN (e.name = '') THEN (NULL) ELSE (e.infogroup_id) END              infogroup_id_benefits_name,
       CASE WHEN (e.row_order IS NULL) THEN (NULL) ELSE (e.infogroup_id) END      infogroup_id_benefits_row_order,
       CASE WHEN (e.short_form = '') THEN (NULL) ELSE (e.infogroup_id) END        infogroup_id_benefits_short_form,
       CASE WHEN (e.term_lower IS NULL) THEN (NULL) ELSE (e.infogroup_id) END     infogroup_id_benefits_term_lower,
       CASE WHEN (e.term_upper IS NULL) THEN (NULL) ELSE (e.infogroup_id) END     infogroup_id_benefits_term_upper,
       CASE
           WHEN (e.total_beginning_participants IS NULL) THEN (NULL)
           ELSE (e.infogroup_id) END                                              infogroup_id_benefits_total_beginning_participants,
       CASE
           WHEN (e.welfare_benefit_types = '') THEN (NULL)
           ELSE (e.infogroup_id) END                                              infogroup_id_benefits_welfare_benefit_types
INTO reports.tableau_benefits_plans_in_business
FROM core_bf.benefit_plans e
         JOIN core_bf.places b ON b.infogroup_id = e.infogroup_id
WHERE in_business = 'yes';

DROP TABLE IF EXISTS reports.tableau_benefits_plans_data_final;

SELECT infogroup_id,
       COUNT(DISTINCT infogroup_id_benefits_ack_id)                        infogroup_id_benefits_ack_id_ben,
       COUNT(DISTINCT infogroup_id_benefits_active_beginning_participants) infogroup_id_benefits_active_beginning_participants_ben,
       COUNT(DISTINCT infogroup_id_benefits_active_ending_participants)    infogroup_id_benefits_active_ending_participants_ben,
       COUNT(DISTINCT infogroup_id_benefits_benefit_arrangements)          infogroup_id_benefits_benefit_arrangements_ben,
       COUNT(DISTINCT infogroup_id_benefits_broker_city)                   infogroup_id_benefits_broker_city_ben,
       COUNT(DISTINCT infogroup_id_benefits_broker_name)                   infogroup_id_benefits_broker_name_ben,
       COUNT(DISTINCT infogroup_id_benefits_broker_postal_code)            infogroup_id_benefits_broker_postal_code_ben,
       COUNT(DISTINCT infogroup_id_benefits_broker_state)                  infogroup_id_benefits_broker_state_ben,
       COUNT(DISTINCT infogroup_id_benefits_broker_street)                 infogroup_id_benefits_broker_street_ben,
       COUNT(DISTINCT infogroup_id_benefits_carrier)                       infogroup_id_benefits_carrier_ben,
       COUNT(DISTINCT infogroup_id_benefits_contract_number)               infogroup_id_benefits_contract_number_ben,
       COUNT(DISTINCT infogroup_id_benefits_effective_date)                infogroup_id_benefits_effective_date_ben,
       COUNT(DISTINCT infogroup_id_benefits_form_id)                       infogroup_id_benefits_form_id_ben,
       COUNT(DISTINCT infogroup_id_benefits_funding_arrangements)          infogroup_id_benefits_funding_arrangements_ben,
       COUNT(DISTINCT infogroup_id_benefits_name)                          infogroup_id_benefits_name_ben,
       COUNT(DISTINCT infogroup_id_benefits_row_order)                     infogroup_id_benefits_row_order,
       COUNT(DISTINCT infogroup_id_benefits_short_form)                    infogroup_id_benefits_short_form_ben,
       COUNT(DISTINCT infogroup_id_benefits_term_lower)                    infogroup_id_benefits_term_lower_ben,
       COUNT(DISTINCT infogroup_id_benefits_term_upper)                    infogroup_id_benefits_term_upper_ben,
       COUNT(DISTINCT infogroup_id_benefits_total_beginning_participants)  infogroup_id_benefits_total_beginning_participants_ben,
       COUNT(DISTINCT infogroup_id_benefits_welfare_benefit_types)         infogroup_id_benefits_welfare_benefit_types_ben
INTO reports.tableau_benefits_plans_data_final
FROM reports.tableau_benefits_plans_in_business
GROUP BY infogroup_id;

DROP TABLE IF EXISTS reports.tableau_raw_data_phase6;

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
       --Transformation of elements for Phase 6.
--Transformation of elements for phase 6 (Several elements are not there)
       -- CASE
       --  WHEN territory = '' THEN 0
       --ELSE 1
       --END infogroup_id_territory,
       CASE
           WHEN credit_grade = '' THEN 0
           ELSE 1
           END                      infogroup_id_credit_grade,
       CASE
           WHEN credit_limit IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_credit_limit,
       CASE
           WHEN credit_score IS NOT NULL THEN 1
           ELSE 0
           END                      infogroup_id_credit_score,
       CASE
           WHEN bankruptcy_dismissal = '' THEN 0
           ELSE 1
           END                      infogroup_id_bankruptcy_dismissal,
       CASE
           WHEN bankruptcy_filing_date = '' THEN 0
           ELSE 1
           END                      infogroup_id_bankruptcy_filing_date,
       CASE
           WHEN bankruptcy_multiple_defendants = '' THEN 0
           ELSE 1
           END                      infogroup_id_bankruptcy_multiple_defendants,
       CASE
           WHEN bankruptcy_release_date = '' THEN 0
           ELSE 1
           END                      infogroup_id_bankruptcy_release_date,
       CASE
           WHEN bankruptcy_filing_type = '' THEN 0
           ELSE 1
           END                      infogroup_id_bankruptcy_filing_type,
--8 vars above
/*UCC data begins*/
       infogroup_id_ucc_internal_record_key_ucc,
       infogroup_id_ucc_amendment_id_ucc,
       infogroup_id_ucc_collateral_types_ucc,
       infogroup_id_ucc_expiration_date_ucc,
       infogroup_id_ucc_filing_date_ucc,
       infogroup_id_ucc_filing_type_ucc,
       infogroup_id_ucc_jurisdiction_ucc,
       infogroup_id_ucc_secured_party_city_ucc,
       infogroup_id_ucc_secured_party_key_ucc,
       infogroup_id_ucc_secured_party_name_ucc,
       infogroup_id_ucc_secured_party_postal_code_ucc,
       infogroup_id_ucc_secured_party_state_ucc,
       infogroup_id_ucc_secured_party_street_ucc,
       infogroup_id_ucc_status_ucc,
       --   CASE
       --  WHEN ucc_filings_count >=1 THEN 1
       --ELSE 0
       --END infogroup_id_ucc_filings_count,
--Benefits
       infogroup_id_benefits_ack_id_ben,
       infogroup_id_benefits_active_beginning_participants_ben,
       infogroup_id_benefits_active_ending_participants_ben,
       infogroup_id_benefits_benefit_arrangements_ben,
       infogroup_id_benefits_broker_city_ben,
       infogroup_id_benefits_broker_name_ben,
       infogroup_id_benefits_broker_postal_code_ben,
       infogroup_id_benefits_broker_state_ben,
       infogroup_id_benefits_broker_street_ben,
       infogroup_id_benefits_carrier_ben,
       infogroup_id_benefits_contract_number_ben,
       infogroup_id_benefits_effective_date_ben,
       infogroup_id_benefits_form_id_ben,
       infogroup_id_benefits_funding_arrangements_ben,
       infogroup_id_benefits_name_ben,
       infogroup_id_benefits_row_order,
       infogroup_id_benefits_short_form_ben,
       infogroup_id_benefits_term_lower_ben,
       infogroup_id_benefits_term_upper_ben,
       infogroup_id_benefits_total_beginning_participants_ben,
       infogroup_id_benefits_welfare_benefit_types_ben
       --    CASE
       --   WHEN benefit_plans_count >=1 THEN 1
       --   ELSE 0
       --  END infogroup_id_benefit_plans_count
INTO reports.tableau_raw_data_phase6
FROM core_bf.v_places_in_business b
         LEFT JOIN reports.professional_title_lk pt
                   ON (b.primary_contact_professional_title = pt.professional_title)
         LEFT JOIN reports.tableau_ucc_filings_data_final u
                   ON (u.infogroup_id = b.infogroup_id)
         LEFT JOIN reports.tableau_benefits_plans_data_final e
                   ON (e.infogroup_id = b.infogroup_id)
WHERE in_business = 'yes';

--select * from workspace.tableau_raw_data_phase5 limit 10;
--select distinct(sic_code_ids)from workspace.tableau_raw_data_phase5
--select  infogroup_id, sic_code_ids_flg from workspace.tableau_raw_data_phase5
--group by sic_code_ids_flg,infogroup_id

DROP TABLE IF EXISTS reports.tableau_extract_phase6;

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
       COUNT(infogroup_id)                                          cnt,
       SUM(infogroup_id_credit_grade)                               cnt_credit_grades,
       SUM(infogroup_id_credit_limit)                               cnt_credit_limit,
       SUM(infogroup_id_credit_score)                               cnt_credit_score,
       SUM(infogroup_id_bankruptcy_dismissal)                       cnt_bankruptcy_dismissal,
       SUM(infogroup_id_bankruptcy_filing_date)                     cnt_bankruptcy_filing_date,
       SUM(infogroup_id_bankruptcy_multiple_defendants)             cnt_bankruptcy_multiple_defendants,
       SUM(infogroup_id_bankruptcy_release_date)                    cnt_bankruptcy_release_date,
       SUM(infogroup_id_bankruptcy_filing_type)                     cnt_bankruptcy_filing_type,
       SUM(infogroup_id_ucc_internal_record_key_ucc)                cnt_internal_record_key_ucc,
       SUM(infogroup_id_ucc_amendment_id_ucc)                       cnt_amendment_id_ucc,
       SUM(infogroup_id_ucc_collateral_types_ucc)                   cnt_collateral_types_ucc,
       SUM(infogroup_id_ucc_expiration_date_ucc)                    cnt_expiration_date_ucc,
       --SUM(infogroup_id_ucc_filings_count) cnt_filings_count_ucc,
       SUM(infogroup_id_ucc_filing_date_ucc)                        cnt_filing_date_ucc,
       SUM(infogroup_id_ucc_filing_type_ucc)                        cnt_filing_type_ucc,
       SUM(infogroup_id_ucc_jurisdiction_ucc)                       cnt_jurisdiction_ucc,
       SUM(infogroup_id_ucc_secured_party_city_ucc)                 cnt_secured_party_city_ucc,
       SUM(infogroup_id_ucc_secured_party_key_ucc)                  cnt_secured_party_key_ucc,
       SUM(infogroup_id_ucc_secured_party_name_ucc)                 cnt_secured_party_name_ucc,
       SUM(infogroup_id_ucc_secured_party_postal_code_ucc)          cnt_secured_party_postal_code_ucc,
       SUM(infogroup_id_ucc_secured_party_state_ucc)                cnt_secured_party_state_ucc,
       SUM(infogroup_id_ucc_secured_party_street_ucc)               cnt_secured_party_street_ucc,
       SUM(infogroup_id_ucc_status_ucc)                             cnt_status_ucc,
       SUM(infogroup_id_benefits_ack_id_ben)                        cnt_benefits_ack_id_ben,
       SUM(infogroup_id_benefits_active_beginning_participants_ben) cnt_benefits_active_beginning_participants_ben,
       SUM(infogroup_id_benefits_active_ending_participants_ben)    cnt_benefits_active_ending_participants_ben,
       SUM(infogroup_id_benefits_benefit_arrangements_ben)          cnt_benefits_benefit_arrangements_ben,
       SUM(infogroup_id_benefits_broker_city_ben)                   cnt_benefits_broker_city_ben,
       SUM(infogroup_id_benefits_broker_name_ben)                   cnt_benefits_broker_name_ben,
       SUM(infogroup_id_benefits_broker_postal_code_ben)            cnt_benefits_broker_postal_code_ben,
       SUM(infogroup_id_benefits_broker_state_ben)                  cnt_benefits_broker_state_ben,
       SUM(infogroup_id_benefits_broker_street_ben)                 cnt_benefits_broker_street_ben,
       SUM(infogroup_id_benefits_carrier_ben)                       cnt_benefits_carrier_ben,
       SUM(infogroup_id_benefits_contract_number_ben)               cnt_benefits_contract_number_ben,
       SUM(infogroup_id_benefits_effective_date_ben)                cnt_benefits_effective_date_ben,
       SUM(infogroup_id_benefits_form_id_ben)                       cnt_benefits_form_id_ben,
       SUM(infogroup_id_benefits_funding_arrangements_ben)          cnt_benefits_funding_arrangements_ben,
       SUM(infogroup_id_benefits_name_ben)                          cnt_benefits_name_ben,
       SUM(infogroup_id_benefits_row_order)                         cnt_benefits_row_order,
       SUM(infogroup_id_benefits_short_form_ben)                    cnt_benefits_short_form_ben,
       SUM(infogroup_id_benefits_term_lower_ben)                    cnt_benefits_term_lower_ben,
       SUM(infogroup_id_benefits_term_upper_ben)                    cnt_benefits_term_upper_ben,
       SUM(infogroup_id_benefits_total_beginning_participants_ben)  cnt_benefits_total_beginning_participants_ben,
       SUM(infogroup_id_benefits_welfare_benefit_types_ben)         cnt_benefits_welfare_benefit_types_ben
       --SUM(infogroup_id_benefit_plans_count) cnt_benefit_plans_cnt
INTO reports.tableau_extract_phase6
FROM reports.tableau_raw_data_phase6
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
         logo_url_ind;

--select infogroup_id_benefits_term_lower_ben, count(1) from reports.tableau_raw_data_phase6
--group by infogroup_id_benefits_term_lower_ben
--1	677288
--1	679628

/*
select infogroup_id_benefits_broker_city_ben, count(1) from reports.tableau_raw_data_phase6
group by infogroup_id_benefits_broker_city_ben
1	95486

select infogroup_id_ucc_secured_party_city_ucc, count(1) from reports.tableau_raw_data_phase6
group by infogroup_id_ucc_secured_party_city_ucc
  */

--1	2495655

--select count(infogroup_id) from  reports.tableau_raw_data_phase6
--17004147

--select max(created_at) from core_bf.places
