DELETE FROM {maintable_name}
WHERE autodb_unique_flag='AU';


INSERT INTO {maintable_name} (
    individual_mc,
    company_mc,
    individual_id,
    company_id,
    address_mc,
    firstname,
    middleinitial,
    lastname,
    gender,
    lemsmatchcode,
    addressline1,
    addressline2,
    city,
    state,
    zip,
    zip_four,
    mail_confidence,
    carrier_route_code,
    delivery_point_bar_code,
    zip9,
    scf,
    preferredcity ,
    fulfillmentflag,
    infogroupcontact,
    gui_probable,
    autodb_unique_flag,
    listid,
    permissiontype
)
SELECT
    max(individual_mc),
    max(Company_mc),
    individual_id,
    --CASE WHEN (max(individual_id) is not null and trim(max(individual_id))<> '') THEN max(individual_id) ELSE individual_mc   END,
    CASE
        WHEN (max(company_id) is not null and max(company_id)<>'') THEN max(company_id) ELSE max(company_mc)
        END,
    max(address_mc),
    max(LTRIM(RTRIM(fname))),
    max(mi),
    max(ltrim(rtrim(lname))),
    max(gender),
    max(AH1_MATCH_CODE),
    max(ah1_local_address),
    max(ah1_secondary_address),
    max(AH1_LONG_CITY_NAME),
    max(AH1_STATE_ABBREVIATION),
    max(AH1_ZIP_CODE),
    max(AH1_ZIP4_CODE),
    max(AH1_MAILABILITY_SCORE),
    max(AH1_CARRIER_ROUTE),
    max(dpb),
    max(LEFT(AH1_ZIP_CODE,5)+LEFT(AH1_ZIP4_CODE,4)),
    max(LEFT(AH1_ZIP_CODE,3)),
    CASE WHEN max(AH1_LONG_CITY_NAME) IS NOT NULL THEN LTRIM(RTRIM(max(AH1_LONG_CITY_NAME))) || ' ' || max(AH1_STATE_ABBREVIATION) ELSE '' END ,
    'A',
    'Y',
    'Y',
    'AU',
    15794,
    'R'
    FROM auto_reformat_infutor a
WHERE a.individual_id <> '' and a.individual_id NOT IN
(SELECT individual_id FROM {maintable_name} GROUP BY individual_id) group by individual_id;



INSERT INTO {maintable_name} (
    individual_mc,
    company_mc,
    individual_id,
    company_id,
    address_mc,
    firstname,
    middleinitial,
    lastname,
    gender,
    lemsmatchcode,
    addressline1,
    addressline2,
    city,
    state,
    zip,
    zip_four,
    mail_confidence,
    carrier_route_code,
    delivery_point_bar_code,
    zip9,
    scf,
    preferredcity ,
    fulfillmentflag,
    infogroupcontact,
    gui_probable,
    autodb_unique_flag,
    listid,
    permissiontype
)
SELECT
    individual_mc,
    max(Company_mc),
    CASE
        WHEN (max(individual_id) is not null and trim(max(individual_id))<> '') THEN max(individual_id) ELSE individual_mc
        END,
    CASE
        WHEN (max(company_id) is not null and max(company_id)<>'') THEN max(company_id) ELSE max(company_mc)
        END,
    max(address_mc),
    max(LTRIM(RTRIM(fname))),
    max(mi),
    max(ltrim(rtrim(lname))),
    max(gender),
    max(AH1_MATCH_CODE),
    max(ah1_local_address),
    max(ah1_secondary_address),
    max(AH1_LONG_CITY_NAME),
    max(AH1_STATE_ABBREVIATION),
    max(AH1_ZIP_CODE),
    max(AH1_ZIP4_CODE),
    max(AH1_MAILABILITY_SCORE),
    max(AH1_CARRIER_ROUTE),
    max(dpb),
    max(LEFT(AH1_ZIP_CODE,5)+LEFT(AH1_ZIP4_CODE,4)),
    max(LEFT(AH1_ZIP_CODE,3)),
    CASE WHEN max(AH1_LONG_CITY_NAME) IS NOT NULL THEN LTRIM(RTRIM(max(AH1_LONG_CITY_NAME))) || ' ' || max(AH1_STATE_ABBREVIATION) ELSE '' END ,
    'A',
    'Y',
    'Y',
    'AU',
    15794,
    'R'
    FROM auto_reformat_infutor a
WHERE (a.individual_id = '' or a.individual_id is null) group by a.individual_mc;

-- -- UPDATE {maintable_name} 
-- -- set hhfield = 
-- -- from {maintable_name} a inner join auto_reformat_infutor b on a.company_id = b.ce_household_ID;

-- -- INSERT INTO {maintable_name} (
-- --     individual_mc,
-- --     company_mc,
-- --     individual_id,
-- --     company_id,
-- --     address_mc,
-- --     firstname,
-- --     middleinitial,
-- --     lastname,
-- --     gender,
-- --     lemsmatchcode,--household fields needs to be added
-- --     addressline1,
-- --     addressline2,
-- --     city,
-- --     state,
-- --     zip,
-- --     zip_four,
-- --     mail_confidence,
-- --     carrier_route_code,
-- --     --lot_size_sqfeet,
-- --     lot_size,
-- --     delivery_point_bar_code,
-- --     zip9,
-- --     scf,
-- --     preferredcity ,
-- --     autodb_unique_flag
-- -- )
-- -- SELECT
-- --     individual_mc,
-- --     Company_mc,
-- --     CASE
-- --         WHEN trim(individual_id)<> '' THEN individual_id ELSE individual_mc
-- --         END,
-- --     CASE
-- --         WHEN company_id<>'' THEN company_id ELSE company_mc
-- --         END,
-- --     address_mc,
-- --     LTRIM(RTRIM(fname)),
-- --     mi,
-- --     lname,
-- --     gender,
-- --     AH1_MATCH_CODE,
-- --     --AH1_LOCAL_ADDRESS,
-- --     --AH1_SECONDARY_ADDRESS,
-- --     AH1_LONG_CITY_NAME,
-- --     AH1_STATE_ABBREVIATION,
-- --     AH1_ZIP_CODE,
-- --     AH1_ZIP4_CODE,
-- --     AH1_MAILABILITY_SCORE,
-- --     AH1_CARRIER_ROUTE,
-- --     AH1_LOT_NUMBER+AH1_LOT_SORTATION_NUMBER,
-- --     dpb,
-- --     LEFT(AH1_ZIP_CODE,5)+LEFT(AH1_ZIP4_CODE,4),
-- --     LEFT(AH1_ZIP_CODE,3),
-- --     CASE WHEN AH1_LONG_CITY_NAME IS NOT NULL THEN LTRIM(RTRIM(AH1_LONG_CITY_NAME)) || ' ' || AH1_STATE_ABBREVIATION ELSE '' END ,
-- --     'AC',
-- --     --HOuse holdfiled case when hhid=companyid then values else 
-- --     FROM auto_reformat_infutor a,{maintable_name} b
-- -- WHERE a.individual_id = '' and a.ce_household_ID = b.company_id;


