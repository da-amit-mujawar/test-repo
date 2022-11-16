DROP TABLE IF EXISTS core_bf.places_changes;

CREATE TABLE core_bf.places_changes
(
change_type varchar(20),
infogroup_id varchar(50) PRIMARY KEY ENCODE RAW, 
place_type varchar(15) default ''  ENCODE ZSTD, 
labels_place_type varchar(15) default '' ENCODE ZSTD, 
work_at_home varchar(11) default '' ENCODE ZSTD,
in_business varchar(5) default '' ENCODE ZSTD, 
labels_in_business varchar(1000) default '' ENCODE ZSTD, 
verified_on varchar(10) default '' ENCODE ZSTD, 
duplicate_of varchar(1000) default '' ENCODE ZSTD, 
suppressed varchar(5) default '' ENCODE ZSTD, 
suppressed_fields varchar(10000) default '' ENCODE ZSTD, 
suppressed_fields_count bigint default 0 ENCODE ZSTD, 
created_at varchar(50) default '' ENCODE ZSTD, 
updated_at varchar(50) default '' ENCODE ZSTD, 
in_business_on varchar(20) default '' ENCODE ZSTD,
opened_for_business_on varchar(10) default '' ENCODE ZSTD, 
estimated_opened_for_business_lower varchar(1000) default '' ENCODE ZSTD, 
estimated_opened_for_business_upper varchar(1000) default '' ENCODE ZSTD, 
out_of_business_on varchar(1000) default '' ENCODE ZSTD, 
express_updated_at varchar(50) default '' ENCODE ZSTD, 
bulk_updated_at varchar(50) default '' ENCODE ZSTD, 
name varchar(1000) default '' ENCODE ZSTD,   --500     --86
alternative_name varchar(10000) default '' ENCODE ZSTD, --500   --159
historical_names varchar(10000) default '' ENCODE ZSTD,   --396
historical_names_count bigint default 0 ENCODE ZSTD, 
legal_names varchar(5000) default '' ENCODE ZSTD,    --439
legal_names_count bigint default 0 ENCODE ZSTD, 
street Varchar(1000) default '' ENCODE ZSTD, 
suite varchar(1000) default '' ENCODE ZSTD, 
city Varchar(500) default '' ENCODE ZSTD,    --50
state Varchar(6) default '' ENCODE ZSTD,
labels_state Varchar(50) default '' ENCODE ZSTD,  --50
postal_code varchar(10) default '' ENCODE ZSTD, 
country_code Varchar(5) default '' ENCODE ZSTD,
labels_country_code Varchar(1000) default '' ENCODE ZSTD, 
territory varchar(15) default '' ENCODE ZSTD,
cross_street_address Varchar(1000) default '' ENCODE ZSTD, 
address_type_code Varchar(1) default '' ENCODE ZSTD, 
labels_address_type_code varchar(1000) default '' ENCODE ZSTD,   --20
address_changed_on varchar(10) default '' ENCODE ZSTD, 
mailing_address Varchar(50) default '' ENCODE ZSTD,   --150
mailing_address_city Varchar(50) default '' ENCODE ZSTD,   --50
mailing_address_state Varchar(2) default '' ENCODE ZSTD, 
labels_mailing_address_state Varchar(40) default '' ENCODE ZSTD, 
mailing_address_postal_code varchar(10) default '' ENCODE ZSTD, 
mailing_address_type_code Varchar(1) default '' ENCODE ZSTD, 
labels_mailing_address_type_code varchar(30) default '' ENCODE ZSTD, 
latitude float default '' ENCODE ZSTD, 
longitude float default '' ENCODE ZSTD, 
geo_match_level Varchar(1) default '' ENCODE ZSTD, 
labels_geo_match_level varchar(1000) default '' ENCODE ZSTD,   --15
geocoordinate_lat float default '' ENCODE ZSTD, 
geocoordinate_lon float default '' ENCODE ZSTD, 
manual_geocoordinate_lat float default '' ENCODE ZSTD, 
manual_geocoordinate_lon float default '' ENCODE ZSTD, 
location_parent_id varchar(15) default '' ENCODE ZSTD,   --20
location_parent_relationship varchar(30) default '' ENCODE ZSTD, 
labels_location_parent_relationship varchar(30) default '' ENCODE ZSTD, 
landmark_address Varchar(150) default '' ENCODE ZSTD,   --150
building_number varchar(10) default '' ENCODE ZSTD, 
number_of_tenants bigint default 0 ENCODE ZSTD, 
primary_sic_code_id varchar(10) default '' ENCODE ZSTD, 
labels_primary_sic_code_id varchar(150) default '' ENCODE ZSTD,   --50
sic_code_ids varchar(20000) default '' ENCODE ZSTD,   --10000
labels_sic_code_ids varchar(20000) default '' ENCODE ZSTD,   --10000
sic_code_ids_count bigint default 0 ENCODE ZSTD, 
primary_naics_code_id varchar(10) default '' ENCODE ZSTD, 
labels_primary_naics_code_id varchar(5000) default '' ENCODE ZSTD, 
naics_code_ids varchar(10000) default '' ENCODE ZSTD, 
labels_naics_code_ids varchar(20000) default '' ENCODE ZSTD, 
naics_code_ids_count bigint default 0 ENCODE ZSTD, 
business_type_ids varchar(1000) default '' ENCODE ZSTD, 
labels_business_type_ids varchar(10000) default '' ENCODE ZSTD, 
business_type_ids_count bigint default 0 ENCODE ZSTD, 
tags_count bigint default 0 ENCODE ZSTD, 
phone varchar(14) default '' ENCODE ZSTD, 
toll_free_number varchar(50) default '' ENCODE ZSTD, 
fax_number varchar(50) default '' ENCODE ZSTD, 
additional_phone varchar(50) default '' ENCODE ZSTD, 
phone_call_status_code varchar(1) default '' ENCODE ZSTD, 
labels_phone_call_status_code Varchar(50) default '' ENCODE ZSTD, 
teleresearch_update_date varchar(50) default '' ENCODE ZSTD, 
location_email_address Varchar(150) default '' ENCODE ZSTD, 
website varchar(10000) default '' ENCODE ZSTD,   --1350
facebook_url varchar(1000) default '' ENCODE ZSTD,    --153
twitter_url varchar(1000) default '' ENCODE ZSTD, 
linked_in_url varchar(1000) default '' ENCODE ZSTD,   --248
yelp_url varchar(1000) default '' ENCODE ZSTD, 
pinterest_url varchar(1000) default '' ENCODE ZSTD, 
youtube_url varchar(1000) default '' ENCODE ZSTD, 
tumblr_url varchar(1000) default '' ENCODE ZSTD, 
foursquare_url varchar(1000) default '' ENCODE ZSTD, 
instagram_url varchar(1000) default '' ENCODE ZSTD, 
logo_url varchar(1000) default '' ENCODE ZSTD,    --262
website_keywords varchar(10000) default '' ENCODE ZSTD, 
website_keywords_count bigint default 0 ENCODE ZSTD, 
primary_contact_id varchar(32) default '' ENCODE ZSTD, 
primary_contact_created_at varchar(50) default '' ENCODE ZSTD, 
primary_contact_email Varchar(1000) default '' ENCODE ZSTD, 
primary_contact_email_md5 Varchar(1000) default '' ENCODE ZSTD, 
primary_contact_email_sha256 Varchar(1000) default '' ENCODE ZSTD, 
primary_contact_email_vendor_id varchar(1000) default '' ENCODE ZSTD, 
primary_contact_email_deliverable varchar(5) default '' ENCODE ZSTD, 
primary_contact_email_marketable varchar(5) default '' ENCODE ZSTD, 
primary_contact_email_reputation_risk varchar(6) default '' ENCODE ZSTD, 
labels_primary_contact_email_reputation_risk varchar(6) default '' ENCODE ZSTD, 
primary_contact_first_name varchar(60) default '' ENCODE ZSTD, 
primary_contact_gender varchar(1) default '' ENCODE ZSTD, 
labels_primary_contact_gender varchar(6) default '' ENCODE ZSTD, 
primary_contact_job_function_id bigint default 0 ENCODE ZSTD, 
labels_primary_contact_job_function_id Varchar(1000) default '' ENCODE ZSTD, 
primary_contact_job_titles varchar(1000) default '' ENCODE ZSTD, 
primary_contact_job_titles_count bigint default 0 ENCODE ZSTD, 
primary_contact_job_title_ids varchar(1000) default '' ENCODE ZSTD, 
primary_contact_job_title_ids_count bigint default 0 ENCODE ZSTD, 
primary_contact_last_name varchar(60) default '' ENCODE ZSTD,    --50
primary_contact_management_level varchar(30) default '' ENCODE ZSTD, 
labels_primary_contact_management_level varchar(1000) default '' ENCODE ZSTD, 
primary_contact_mapped_contact_id varchar(15) default '' ENCODE ZSTD, 
primary_contact_professional_title varchar(10) default '' ENCODE ZSTD, 
labels_primary_contact_professional_title Varchar(1000) default '' ENCODE ZSTD, 
primary_contact_primary varchar(5) default '' ENCODE ZSTD, 
primary_contact_suppressed_fields varchar(1000) default '' ENCODE ZSTD, 
primary_contact_suppressed_fields_count bigint default 0 ENCODE AZ64, 
primary_contact_title_codes varchar(1000) default '' ENCODE ZSTD, 
labels_primary_contact_title_codes Varchar(10000) default '' ENCODE ZSTD, 
primary_contact_title_codes_count bigint default 0 ENCODE ZSTD, 
primary_contact_vendor_id varchar(10) default '' ENCODE ZSTD,    --4
contacts_count bigint default 0 ENCODE ZSTD, 
ownership_changed_on varchar(10) default '' ENCODE ZSTD, 
location_employee_count bigint default 0 ENCODE ZSTD, 
estimated_location_employee_count bigint default 0 ENCODE ZSTD, 
location_professional_size_code varchar(1) default '' ENCODE ZSTD, 
labels_location_professional_size_code varchar(1000) default '' ENCODE ZSTD, 
headquarters_id varchar(15) default '' ENCODE ZSTD, 
ancestor_headquarters_ids varchar(1000) default '' ENCODE ZSTD, 
ancestor_headquarters_ids_count bigint default 0 ENCODE ZSTD, 
ultimate_headquarters_id varchar(15) default '' ENCODE ZSTD, 
chain_id varchar(1000) default '' ENCODE ZSTD, 
labels_chain_id varchar(1000) default '' ENCODE ZSTD, 
corporate_franchising varchar(5) default '' ENCODE ZSTD, 
corporate_employee_count bigint default 0 ENCODE ZSTD, 
estimated_corporate_employee_count bigint default 0 ENCODE ZSTD, 
corporate_sales_revenue bigint default 0  ENCODE ZSTD, 
estimated_corporate_sales_revenue bigint default 0 ENCODE ZSTD, 
branch_count bigint default 0 ENCODE ZSTD, 
foreign_parent_flag varchar(5)  ENCODE ZSTD, 
fortune_ranking bigint default 0 ENCODE AZ64, 
fiscal_year_end_month varchar(10) default '' ENCODE ZSTD, 
labels_fiscal_year_end_month varchar(1000) default '' ENCODE ZSTD, 
stock_exchange_code varchar(40) default '' ENCODE ZSTD, 
labels_stock_exchange_code varchar(1000) default '' ENCODE ZSTD, 
stock_ticker_symbol varchar(40) default '' ENCODE ZSTD, 
cik varchar(40) default '' ENCODE ZSTD,
eins_count bigint default 0 ENCODE ZSTD, 
affiliation_ids varchar(1000) default '' ENCODE ZSTD,    --10000
labels_affiliation_ids varchar(1000) default '' ENCODE ZSTD, 
affiliation_ids_count bigint default 0 ENCODE ZSTD, 
brand_ids varchar(10000) default '' ENCODE ZSTD, 
labels_brand_ids varchar(10000) default '' ENCODE ZSTD,   --10000
brand_ids_count bigint default 0 ENCODE ZSTD, 
company_description varchar(20000) default '' ENCODE ZSTD,   ---10000
dress_code varchar(1000) default '' ENCODE ZSTD, 
equipment_rentals varchar(5) default '' ENCODE ZSTD, 
has_ecommerce varchar(5) default '' ENCODE ZSTD, 
languages_spoken varchar(1000) default '' ENCODE ZSTD,  --10000
labels_languages_spoken varchar(1000) default '' ENCODE ZSTD,  --10000
languages_spoken_count bigint default 0 ENCODE ZSTD, 
price_range varchar(20000) default '' ENCODE ZSTD, 
professional_specialty_ids varchar(20000) default ''  ENCODE ZSTD, 
labels_professional_specialty_ids varchar(10000) default '' ENCODE ZSTD, 
professional_specialty_ids_count bigint default 0 ENCODE ZSTD, 
public_access varchar(5) default '' ENCODE ZSTD, 
shopping_center_atm varchar(5) default '' ENCODE ZSTD, 
federal_government_contractor varchar(5) default '' ENCODE ZSTD, 
operating_hours_description varchar(1000) default '' ENCODE ZSTD, 
operating_hours_count bigint default 0 ENCODE ZSTD, 
payment_types varchar(1000) default '' ENCODE ZSTD, 
labels_payment_types varchar(1000) default '' ENCODE ZSTD, 
payment_types_count bigint default 0 ENCODE ZSTD, 
insurances_accepted varchar(10000) default '' ENCODE ZSTD, 
insurances_accepted_count bigint default 0 ENCODE ZSTD, 
medicare_accepted varchar(5) default '' ENCODE ZSTD, 
medicaid_accepted varchar(5) default '' ENCODE ZSTD, 
images_count bigint default 0 ENCODE ZSTD, 
cbsa_code varchar(10) default '' ENCODE ZSTD, 
labels_cbsa_code varchar(1000) default '' ENCODE ZSTD,    --1000
cbsa_level varchar(1) default '' ENCODE ZSTD, 
labels_cbsa_level varchar(250) default '' ENCODE ZSTD,  --1000
csa_code varchar(5) default '' ENCODE ZSTD, 
labels_csa_code varchar(250) default '' ENCODE ZSTD, 
census_block varchar(1) default '' ENCODE ZSTD, 
census_tract varchar(10) default '' ENCODE ZSTD, 
carrier_route_code varchar(4) default '' ENCODE BYTEDICT, 
mailing_address_carrier_route_code varchar(4) default '' ENCODE ZSTD, 
fips_code varchar(10) default '' ENCODE ZSTD, 
labels_fips_code varchar(1000) default '' ENCODE ZSTD,    --250
county_code varchar(4) default '' ENCODE ZSTD, 
sgc_division varchar(4) default '' ENCODE ZSTD, 
labels_sgc_division varchar(1000) default '' ENCODE ZSTD,   --250
delivery_point_bar_code varchar(4) default '' ENCODE ZSTD, 
mailing_address_delivery_point_bar_code varchar(4) default '' ENCODE ZSTD, 
mailing_score_code varchar(2) default '' ENCODE ZSTD, 
labels_mailing_score_code varchar(1000) default '' ENCODE ZSTD, 
mailing_address_score_code varchar(2) default '' ENCODE ZSTD, 
labels_mailing_address_score_code varchar(1000) default '' ENCODE ZSTD, 
zip varchar(5) default '' ENCODE ZSTD,    --10
zip_four varchar(4) default '' ENCODE ZSTD, 
mailing_address_zip varchar(5) default '' ENCODE ZSTD,  --10
mailing_address_zip_four varchar(4) default '' ENCODE ZSTD, 
cmra_flag varchar(5) default '' ENCODE ZSTD, 
mailing_address_cmra_flag varchar(5) default '' ENCODE ZSTD, 
parking_id varchar(1000) default '' ENCODE ZSTD, 
parking_created_at varchar(50) default '' ENCODE ZSTD, 
parking_bike_rack varchar(5) default '' ENCODE ZSTD, 
parking_bike_lockers varchar(5) default '' ENCODE ZSTD, 
parking_electric_charging_port_count bigint default 0 ENCODE ZSTD, 
parking_free varchar(5) default '' ENCODE ZSTD, 
parking_number_of_spaces bigint default 0 ENCODE AZ64, 
parking_onsite varchar(5) default '' ENCODE ZSTD, 
parking_overnight_parking varchar(5) default '' ENCODE ZSTD, 
parking_permit_required varchar(5) default '' ENCODE ZSTD, 
parking_public varchar(5) default '' ENCODE ZSTD, 
parking_transit_lines varchar(1000) default '' ENCODE ZSTD, 
parking_valet varchar(1000) default '' ENCODE ZSTD, 
credit_id varchar(1000) default '' ENCODE ZSTD, 
credit_created_at varchar(1000) default '' ENCODE ZSTD, 
credit_grade varchar(2) default '' ENCODE ZSTD, 
labels_credit_grade varchar(1000) default '' ENCODE ZSTD, 
credit_limit bigint default 0 ENCODE ZSTD, 
credit_score bigint default 0 ENCODE ZSTD, 
bankruptcy_id varchar(1000) default '' ENCODE ZSTD, 
bankruptcy_created_at varchar(1000) default '' ENCODE ZSTD, 
bankruptcy_dismissal varchar(1000) default '' ENCODE ZSTD, 
bankruptcy_filing_date varchar(1000) default ''  ENCODE ZSTD, 
bankruptcy_multiple_defendants varchar(5) default '' ENCODE ZSTD, 
bankruptcy_release_date varchar(1000) default '' ENCODE ZSTD, 
bankruptcy_filing_type varchar(1000) default '' ENCODE ZSTD, 
labels_bankruptcy_filing_type varchar(1000) default '' ENCODE ZSTD,   --150
ucc_filings_count bigint default 0 ENCODE ZSTD, 
benefit_plans_count bigint default 0 ENCODE ZSTD, 
nbrc_corporation_filing_type varchar(1000) default '' ENCODE ZSTD, 
nbrc_corporate_date varchar(1000) default '' ENCODE ZSTD, 
location_intents_count bigint default 0 ENCODE ZSTD, 
car_make_ids varchar(1000) default '' ENCODE ZSTD, 
labels_car_make_ids varchar(1000) default '' ENCODE ZSTD, 
car_make_ids_count bigint default 0 ENCODE ZSTD, 
religious_denomination_ids varchar(500) default '' ENCODE ZSTD, --1000
labels_religious_denomination_ids varchar(1000) default ''  ENCODE ZSTD,   --1000
religious_denomination_ids_count bigint default 0 ENCODE ZSTD, 
restaurant_id varchar(1000) default '' ENCODE ZSTD, 
restaurant_created_at varchar(1000) default '' ENCODE ZSTD, 
restaurant_cuisines varchar(10000) default '' ENCODE ZSTD, 
labels_restaurant_cuisines varchar(10000) default '' ENCODE ZSTD, 
restaurant_cuisines_count bigint default 0 ENCODE ZSTD, 
restaurant_limited_service varchar(5) default '' ENCODE ZSTD, 
restaurant_reservations varchar(5) default '' ENCODE ZSTD, 
restaurant_takeout varchar(5) default '' ENCODE ZSTD, 
happy_hours_count bigint default 0 ENCODE ZSTD, 
hotel_id varchar(1000) default '' ENCODE ZSTD, 
hotel_created_at varchar(50) default '' ENCODE ZSTD, 
hotel_cable_tv varchar(5) default '' ENCODE ZSTD, 
hotel_continental_breakfast varchar(5) default '' ENCODE ZSTD, 
hotel_elevator varchar(5) default '' ENCODE ZSTD, 
hotel_exercise_facility varchar(5) default '' ENCODE ZSTD, 
hotel_guest_laundry varchar(5) default '' ENCODE ZSTD, 
hotel_hot_tub varchar(5) default '' ENCODE ZSTD, 
hotel_indoor_pool varchar(5) default '' ENCODE ZSTD, 
hotel_kitchens varchar(5) default '' ENCODE ZSTD, 
hotel_outdoor_pool varchar(5) default '' ENCODE ZSTD, 
hotel_pet_friendly varchar(5) default '' ENCODE ZSTD, 
hotel_room_service varchar(5) default '' ENCODE ZSTD, 
hospital_has_emergency_room varchar(5) default '' ENCODE ZSTD, 
ami_physician_primary_specialty_code bigint default 0 ENCODE ZSTD, 
labels_ami_physician_primary_specialty_code varchar(1000) default '' ENCODE ZSTD,   --150
ami_physician_secondary_specialty_code bigint default 0  ENCODE ZSTD, 
labels_ami_physician_secondary_specialty_code varchar(1000) default '' ENCODE ZSTD,  --150
ami_board_certified_flag varchar(5) default '' ENCODE ZSTD,  
physician_year_of_graduation bigint default 0 ENCODE ZSTD, 
ami_medical_school_code varchar(1000) default '' ENCODE ZSTD, 
labels_ami_medical_school_code varchar(1000) default '' ENCODE ZSTD, 
ami_hospital_number varchar(10) default '' ENCODE ZSTD, 
labels_ami_hospital_number varchar(1000) default '' ENCODE ZSTD, 
upin varchar(6) default '' ENCODE ZSTD, 
ami_license_number varchar(1000) default '' ENCODE ZSTD, 
ami_state_of_license varchar(2) default '' ENCODE ZSTD, 
labels_ami_state_of_license Varchar(1000) default '' ENCODE ZSTD, 
physician_year_of_birth bigint default 0 ENCODE ZSTD, 
ami_residency_hospital_code varchar(1000) default '' ENCODE ZSTD, 
labels_ami_residency_hospital_code Varchar(1000) default '' ENCODE ZSTD, 
residency_graduation_year bigint default 0 ENCODE ZSTD, 
ami_national_provider_flag varchar(30) default '' ENCODE ZSTD,   --20
medical_dea_number varchar(1000) default '' ENCODE ZSTD,  --20
accepting_new_patients varchar(1000) default '' ENCODE ZSTD, 
estimated_fleet_size_lower bigint default 0 ENCODE ZSTD, 
estimated_fleet_size_upper bigint default 0 ENCODE ZSTD, 
greenscore bigint default 0 ENCODE ZSTD, 
growing_business_code varchar(1) default '' ENCODE ZSTD, 
labels_growing_business_code varchar(1000) default '' ENCODE ZSTD, 
location_sales_volume bigint default 0 ENCODE ZSTD, 
square_footage varchar(1) default '' ENCODE ZSTD, 
labels_square_footage varchar(1000)  ENCODE ZSTD, 
white_collar_percentage bigint default 0 ENCODE ZSTD, 
expenses_id varchar(1000) default '' ENCODE ZSTD, 
expenses_created_at varchar(1000) default '' ENCODE ZSTD, 
expenses_accounting_lower bigint default 0 ENCODE ZSTD, 
expenses_accounting_upper bigint default 0 ENCODE ZSTD, 
expenses_advertising_lower bigint default 0 ENCODE ZSTD, 
expenses_advertising_upper bigint default 0 ENCODE ZSTD, 
expenses_charities_lower bigint default 0 ENCODE ZSTD, 
expenses_charities_upper bigint default 0 ENCODE ZSTD, 
expenses_contract_labor_lower bigint default 0 ENCODE ZSTD, 
expenses_contract_labor_upper bigint default 0 ENCODE ZSTD, 
expenses_corporate varchar(5) default '' ENCODE ZSTD, 
expenses_insurance_lower bigint default 0 ENCODE ZSTD, 
expenses_insurance_upper bigint default 0 ENCODE ZSTD, 
expenses_legal_lower bigint default 0 ENCODE ZSTD, 
expenses_legal_upper bigint default 0 ENCODE ZSTD, 
expenses_licenses_lower bigint default 0 ENCODE ZSTD, 
expenses_licenses_upper bigint default 0 ENCODE ZSTD, 
expenses_maintenance_lower bigint default 0 ENCODE ZSTD, 
expenses_maintenance_upper bigint default 0 ENCODE ZSTD, 
expenses_office_equipment_supplies_lower bigint default 0 ENCODE ZSTD, 
expenses_office_equipment_supplies_upper bigint default 0 ENCODE ZSTD, 
expenses_packaging_shipping_lower bigint default 0 ENCODE ZSTD, 
expenses_packaging_shipping_upper bigint default 0 ENCODE ZSTD, 
expenses_payroll_lower bigint default 0 ENCODE ZSTD, 
expenses_payroll_upper bigint default 0 ENCODE ZSTD, 
expenses_printing_lower bigint default 0 ENCODE ZSTD, 
expenses_printing_upper bigint default 0 ENCODE ZSTD, 
expenses_professional_services_lower bigint default 0 ENCODE ZSTD, 
expenses_professional_services_upper bigint default 0 ENCODE ZSTD, 
expenses_rent_lease_lower bigint default 0 ENCODE ZSTD, 
expenses_rent_lease_upper bigint default 0 ENCODE ZSTD, 
expenses_technology_lower bigint default 0 ENCODE ZSTD,
expenses_technology_upper bigint default 0 ENCODE ZSTD, 
expenses_telecommunications_lower bigint default 0 ENCODE ZSTD, 
expenses_telecommunications_upper bigint default 0 ENCODE ZSTD, 
expenses_transportation_lower bigint default 0 ENCODE ZSTD, 
expenses_transportation_upper bigint default 0 ENCODE ZSTD, 
expenses_utilities_lower bigint default 0 ENCODE ZSTD, 
expenses_utilities_upper bigint default 0 ENCODE ZSTD, 
population_density float default '' ENCODE ZSTD, 
population_code_for_zip varchar(1) default '' ENCODE ZSTD, 
labels_population_code_for_zip varchar(1000) default '' ENCODE ZSTD, 
wealthy_area_flag varchar(5) default '' ENCODE ZSTD
)
diststyle ALL
;