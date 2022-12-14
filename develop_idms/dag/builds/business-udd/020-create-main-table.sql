DROP TABLE IF EXISTS udd_main_table;
CREATE TABLE udd_main_table (
    id bigint DISTKEY SORTKEY,
    listid int ENCODE zstd DEFAULT 19857,
    infogroup_id varchar(9) ENCODE zstd,
    contact_id varchar(32) ENCODE zstd,
    contact_created_at varchar(20) ENCODE zstd,
    first_name varchar(43) ENCODE zstd,
    last_name varchar(42) ENCODE zstd,
    professional_title varchar(9) ENCODE zstd,
    gender varchar(1) ENCODE zstd,
    management_level varchar(22) ENCODE zstd,
    mapped_contact_id varchar(12) ENCODE zstd,
    vendor_id varchar(2) ENCODE zstd,
    job_function_id integer ENCODE az64,
    job_titles_count integer ENCODE zstd,
    job_title varchar(155) ENCODE zstd,
    contact_suppressed_fields_count integer ENCODE zstd,
    title_codes_count integer ENCODE zstd,
    is_primary boolean ENCODE zstd,
    email varchar(63) ENCODE zstd,
    email_md5 varchar(32) ENCODE zstd,
    email_sha256 varchar(64) ENCODE zstd,
    email_vendor_id varchar(2) ENCODE zstd,
    email_deliverable boolean ENCODE zstd,
    email_marketable boolean ENCODE zstd,
    email_reputation_risk varchar(6) ENCODE zstd,
    -- Places Start Here
    places_created_at varchar(20) ENCODE zstd,
    places_updated_at varchar(20) ENCODE zstd,
    local_listings_premium_claimed_at varchar(20) ENCODE zstd,
    local_listings_claimed_at varchar(20) ENCODE zstd,
    in_business_determined_at varchar(20) ENCODE zstd,
    verified_on date ENCODE zstd,
    ownership_changed_on date ENCODE zstd,
    out_of_business_on date ENCODE zstd,
    opened_for_business_on date ENCODE zstd,
    address_changed_on date ENCODE zstd,
    work_at_home boolean ENCODE zstd,
    suppressed boolean ENCODE zstd,
    territory boolean ENCODE zstd,
    public_access boolean ENCODE zstd,
    medicaid_accepted boolean ENCODE zstd,
    medicare_accepted boolean ENCODE zstd,
    cmra_flag boolean ENCODE zstd,
    mailing_address_cmra_flag boolean ENCODE zstd,
    hospital_has_emergency_room boolean ENCODE zstd,
    has_ecommerce boolean ENCODE zstd,
    foreign_parent_flag boolean ENCODE zstd,
    equipment_rentals boolean ENCODE zstd,
    corporate_franchising boolean ENCODE zstd,
    additional_phone varchar(14) ENCODE zstd,
    address_type_code varchar(1) ENCODE zstd,
    alternative_name varchar(1259) ENCODE zstd,
    branch_type_id integer ENCODE zstd,
    carrier_route_code varchar(4) ENCODE zstd,
    cbsa_code varchar(5) ENCODE zstd,
    cbsa_level varchar(1) ENCODE zstd,
    census_block varchar(1) ENCODE zstd,
    census_tract varchar(6) ENCODE zstd,
    chain_id varchar(36) ENCODE zstd,
    cik varchar(11) ENCODE zstd,
    city varchar(45) ENCODE zstd,
    corporate_employee_count integer ENCODE zstd,
    corporate_sales_revenue bigint ENCODE zstd,
    country_code varchar(14) ENCODE zstd,
    cross_street_address varchar(492) ENCODE zstd,
    csa_code varchar(3) ENCODE zstd,
    delivery_point_bar_code varchar(11) ENCODE zstd,
    dma_county_rank integer ENCODE zstd,
    dma_region varchar(3) ENCODE zstd,
    dma_territory varchar(19) ENCODE zstd,
    dress_code varchar(984) ENCODE zstd,
    duplicate_of varchar(9) ENCODE zstd,
    estimated_corporate_employee_count integer ENCODE zstd,
    estimated_corporate_sales_revenue bigint ENCODE zstd,
    estimated_location_employee_count integer ENCODE zstd,
    facebook_url varchar(4190) ENCODE zstd,
    fax_number varchar(15) ENCODE zstd,
    fips_code varchar(20) ENCODE zstd,
    fiscal_year_end_month varchar(2) ENCODE zstd,
    fortune_ranking integer ENCODE zstd,
    foursquare_url varchar(261) ENCODE zstd,
    geo_match_level varchar(1) ENCODE zstd,
    headquarters_id varchar(44) ENCODE zstd,
    in_business varchar(5) ENCODE zstd,
    in_business_research varchar(11) ENCODE zstd,
    instagram_url varchar(350) ENCODE zstd,
    landmark_address varchar(317) ENCODE zstd,
    latitude float ENCODE zstd,
    linked_in_url varchar(310) ENCODE zstd,
    location_email_address varchar(102) ENCODE zstd,
    location_employee_count integer ENCODE zstd,
    location_sales_volume bigint ENCODE zstd,
    logo_url varchar(691) ENCODE zstd,
    longitude float ENCODE zstd,
    mailing_address varchar(58) ENCODE zstd,
    mailing_address_carrier_route_code varchar(4) ENCODE zstd,
    mailing_address_city varchar(34) ENCODE zstd,
    mailing_address_delivery_point_bar_code varchar(3) ENCODE zstd,
    mailing_address_postal_code varchar(10) ENCODE zstd,
    mailing_address_score_code varchar(2) ENCODE zstd,
    mailing_address_state varchar(2) ENCODE zstd,
    mailing_address_type_code varchar(1) ENCODE zstd,
    mailing_address_zip varchar(5) ENCODE zstd,
    mailing_address_zip_four varchar(4) ENCODE zstd,
    mailing_score_code varchar(2) ENCODE zstd,
    name varchar(165) ENCODE zstd,
    neighborhood varchar(38) ENCODE zstd,
    operating_hours_description varchar(316) ENCODE zstd,
    phone varchar(14) ENCODE zstd,
    phone_call_status_code varchar(1) ENCODE zstd,
    pinterest_url varchar(175) ENCODE zstd,
    place_type varchar(12) ENCODE zstd,
    postal_code varchar(10) ENCODE zstd,
    price_range varchar(256) ENCODE zstd,
    primary_naics_code_id varchar(8) ENCODE zstd,
    primary_sic_code_id varchar(6) ENCODE zstd,
    segment_id integer ENCODE zstd,
    sgc_division varchar(4) ENCODE zstd,
    square_footage varchar(1) ENCODE zstd,
    state varchar(2) ENCODE zstd,
    stock_exchange_code varchar(1) ENCODE zstd,
    stock_ticker_symbol varchar(6) ENCODE zstd,
    street varchar(126) ENCODE zstd,
    suite varchar(53) ENCODE zstd,
    teleresearch_update_date varchar(21) ENCODE bytedict,
    tumblr_url varchar(74) ENCODE zstd,
    twitter_url varchar(198) ENCODE zstd,
    yelp_url varchar(147) ENCODE zstd,
    youtube_url varchar(366) ENCODE zstd,
    toll_free_number varchar(15) ENCODE zstd,
    ultimate_headquarters_id varchar(9) ENCODE zstd,
    website varchar(6189) ENCODE zstd,
    website_status varchar(23) ENCODE zstd,
    zip varchar(7) ENCODE zstd,
    zip_four varchar(4) ENCODE zstd,
    happy_hours_count integer ENCODE zstd,
    historical_names_count integer ENCODE zstd,
    images_count integer ENCODE zstd,
    insurances_accepted_count integer ENCODE zstd,
    languages_spoken_count integer ENCODE zstd,
    legal_names_count integer ENCODE zstd,
    location_intents_count integer ENCODE zstd,
    naics_code_ids_count integer ENCODE zstd,
    operating_hours_count integer ENCODE zstd,
    payment_types_count integer ENCODE zstd,
    preformatted_fields_count integer ENCODE zstd,
    record_list_ids_count integer ENCODE zstd,
    religious_denomination_ids_count integer ENCODE zstd,
    sic_code_ids_count integer ENCODE zstd,
    places_suppressed_fields_count integer ENCODE az64,
    tax_ids_count integer ENCODE zstd,
    ucc_filings_count integer ENCODE zstd,
    website_keywords_count integer ENCODE zstd,
    ancestor_headquarters_ids_count integer ENCODE zstd,
    affiliation_ids_count integer ENCODE zstd,
    bankruptcies_count integer ENCODE zstd,
    benefit_plans_count integer ENCODE zstd,
    branch_count integer ENCODE zstd,
    brand_ids_count integer ENCODE zstd,
    business_type_ids_count integer ENCODE zstd,
    car_make_ids_count integer ENCODE zstd,
    contacts_count integer ENCODE zstd
);