DROP TABLE IF EXISTS {maintable_name};

CREATE TABLE {maintable_name} (
    id bigint ENCODE az64 DISTKEY SORTKEY,
    abinumber character varying(9) ENCODE zstd,
    company_old character varying(30) ENCODE zstd,
    company character varying(30) ENCODE zstd,
    address character varying(30) ENCODE zstd,
    city character varying(16) ENCODE zstd,
    state character varying(2) ENCODE bytedict,
    zip character varying(5) ENCODE zstd,
    zipfour character varying(4) ENCODE zstd,
    zip9 character varying(9) ENCODE zstd,
    carrierroutecode character varying(4) ENCODE bytedict,
    mailing_address character varying(30) ENCODE zstd,
    mailing_city character varying(16) ENCODE zstd,
    mailing_state character varying(2) ENCODE bytedict,
    mailing_zipcode character varying(5) ENCODE zstd,
    mailing_zipfour character varying(4) ENCODE zstd,
    mailing_carrieroutecode character varying(4) ENCODE bytedict,
    landmark_address character varying(30) ENCODE zstd,
    landmark_city character varying(16) ENCODE zstd,
    landmark_state character varying(2) ENCODE zstd,
    landmark_zipcode character varying(5) ENCODE zstd,
    landmark_zipfour character varying(4) ENCODE zstd,
    landmark_carrieroutecode character varying(4) ENCODE zstd,
    countycode character varying(3) ENCODE bytedict,
    censustract character varying(6) ENCODE zstd,
    censusblock character varying(1) ENCODE zstd,
    flatitude double precision ENCODE raw,
    flongitude double precision ENCODE raw,
    matchcode character varying(1) ENCODE zstd,
    tollfreephonenumber character varying(10) ENCODE zstd,
    radialdistance character varying(4) ENCODE zstd,
    phonenumber character varying(10) ENCODE zstd,
    faxphone character varying(10) ENCODE zstd,
    primarysic6 character varying(6) ENCODE zstd,
    primarysic4 character varying(4) ENCODE zstd,
    primarysic2 character varying(2) ENCODE bytedict,
    primarysicdesiginator character varying(1) ENCODE zstd,
    siccode1 character varying(6) ENCODE zstd,
    siccode2 character varying(6) ENCODE zstd,
    siccode3 character varying(6) ENCODE zstd,
    siccode4 character varying(6) ENCODE zstd,
    franchisecode1 character varying(1) ENCODE zstd,
    franchisecode2 character varying(1) ENCODE zstd,
    franchisecode3 character varying(1) ENCODE zstd,
    franchisecode4 character varying(1) ENCODE zstd,
    franchisecode5 character varying(1) ENCODE zstd,
    franchisecode6 character varying(1) ENCODE zstd,
    truefranchiseflag character varying(1) ENCODE zstd,
    industryspecificfirstbyte character varying(1) ENCODE zstd,
    naicscode character varying(8) ENCODE zstd,
    numberemployees integer ENCODE az64,
    employeesizecode2 character varying(1) ENCODE zstd,
    outputvolume2 integer ENCODE az64,
    outputvolumecode2 character varying(1) ENCODE zstd,
    adsizecode character varying(1) ENCODE zstd,
    wealthcode character varying(1) ENCODE zstd,
    bigbusinesssegmentationcode character varying(1) ENCODE zstd,
    creditnumericscore character varying(3) ENCODE bytedict,
    creditalphascore character varying(2) ENCODE zstd,
    businessstatuscode character varying(1) ENCODE zstd,
    callstatuscode character varying(1) ENCODE zstd,
    growingbusinessflag character varying(1) ENCODE zstd,
    smmedbusseg1 character varying(1) ENCODE zstd,
    smmedbusseg2 character varying(1) ENCODE zstd,
    fortuneranking character varying(4) ENCODE zstd,
    cottagecode character varying(1) ENCODE zstd,
    hightechbusinessflag character varying(1) ENCODE zstd,
    identificationcode character varying(1) ENCODE zstd,
    officesizecode character varying(1) ENCODE zstd,
    ownorlease character varying(1) ENCODE zstd,
    filingflag character varying(1) ENCODE zstd,
    companyholdingstatus character varying(1) ENCODE zstd,
    stockexchangecode character varying(1) ENCODE zstd,
    stocktickersymbol character varying(6) ENCODE zstd,
    teleresearchupdatedate character varying(6) ENCODE bytedict,
    parentabinumber character varying(9) ENCODE zstd,
    subsidairyabinumber character varying(9) ENCODE zstd,
    sitenumber character varying(9) ENCODE zstd,
    webaddress character varying(40) ENCODE zstd,
    yellowpagecode character varying(5) ENCODE zstd,
    booknumber character varying(5) ENCODE zstd,
    dateindatabase character varying(6) ENCODE zstd,
    yearfirstappeared character varying(4) ENCODE bytedict,
    yearestablished character varying(4) ENCODE zstd,
    deliverypointbarcode1 character varying(3) ENCODE zstd,
    deliverypointbarcode2 character varying(3) ENCODE zstd,
    modeledemployeesizeflag character varying(1) ENCODE zstd,
    outputvolumetypecode character varying(1) ENCODE zstd,
    acronymflag character varying(1) ENCODE zstd,
    currenttransactioncode character varying(1) ENCODE zstd,
    location_parsedaddresstype character varying(1) ENCODE zstd,
    location_parsedzipcode character varying(5) ENCODE zstd,
    location_parsedstreetname character varying(28) ENCODE zstd,
    location_parsedsuffix character varying(5) ENCODE zstd,
    location_parsedpredirectional character varying(2) ENCODE zstd,
    location_parsedpostdirection character varying(2) ENCODE zstd,
    location_parsedhousenumber character varying(15) ENCODE zstd,
    location_parsedsuitenumber character varying(20) ENCODE zstd,
    location_parsedcarrieroute character varying(4) ENCODE bytedict,
    multitenantcode character varying(1) ENCODE zstd,
    buildingnumber character varying(7) ENCODE zstd,
    multitenantnumber character varying(7) ENCODE zstd,
    propertymanagementabi character varying(9) ENCODE zstd,
    propertymgmt_businessname character varying(30) ENCODE zstd,
    propertymgmt_address character varying(30) ENCODE zstd,
    propertymgmt_city character varying(16) ENCODE zstd,
    propertymgmt_state character varying(2) ENCODE zstd,
    propertymgmt_zipcode character varying(5) ENCODE zstd,
    propertymgmt_zipfour character varying(4) ENCODE zstd,
    propertymgmt_firstname character varying(11) ENCODE zstd,
    propertymgmt_lastname character varying(14) ENCODE zstd,
    whitecollar character varying(3) ENCODE bytedict,
    whitecollarflag character varying(1) ENCODE zstd,
    cbsacode character varying(5) ENCODE zstd,
    mailconfidence character varying(2) ENCODE zstd,
    mailconfidence_mailing character varying(1) ENCODE zstd,
    cbsalevel character varying(1) ENCODE zstd,
    csacode character varying(3) ENCODE bytedict,
    governmentflag character varying(1) ENCODE zstd,
    foreignparentflag character varying(1) ENCODE zstd,
    importexportflag character varying(1) ENCODE zstd,
    mondayopen character varying(4) ENCODE zstd,
    mondayclose character varying(4) ENCODE zstd,
    tuesdayopen character varying(4) ENCODE zstd,
    tuesdayclose character varying(4) ENCODE zstd,
    wednesdayopen character varying(4) ENCODE zstd,
    wednesdayclose character varying(4) ENCODE zstd,
    thursdayopen character varying(4) ENCODE zstd,
    thursdayclose character varying(4) ENCODE zstd,
    fridayopen character varying(4) ENCODE zstd,
    fridayclose character varying(4) ENCODE zstd,
    saturdayopen character varying(4) ENCODE zstd,
    saturdayclose character varying(4) ENCODE zstd,
    sundayopen character varying(4) ENCODE zstd,
    sundayclose character varying(4) ENCODE zstd,
    payatpump character varying(1) ENCODE zstd,
    creditcardsaccepted_1 character varying(1) ENCODE zstd,
    creditcardsaccepted_2 character varying(1) ENCODE zstd,
    creditcardsaccepted_3 character varying(1) ENCODE zstd,
    creditcardsaccepted_4 character varying(1) ENCODE zstd,
    creditcardsaccepted_5 character varying(1) ENCODE zstd,
    creditcardsaccepted_6 character varying(1) ENCODE zstd,
    creditcardsaccepted_7 character varying(1) ENCODE zstd,
    creditcardsaccepted_8 character varying(1) ENCODE zstd,
    creditcardsaccepted_9 character varying(1) ENCODE zstd,
    creditcardsaccepted_10 character varying(1) ENCODE zstd,
    creditcardsaccepted_11 character varying(1) ENCODE zstd,
    creditcardsaccepted_12 character varying(1) ENCODE zstd,
    creditcardsaccepted_13 character varying(1) ENCODE zstd,
    creditcardsaccepted_14 character varying(1) ENCODE zstd,
    creditcardsaccepted_15 character varying(1) ENCODE zstd,
    suspectreasoncode character varying(1) ENCODE zstd,
    telecommunications_expense character varying(1) ENCODE zstd,
    technology_expense character varying(1) ENCODE zstd,
    office_equipment_expense character varying(1) ENCODE zstd,
    payroll_expense character varying(1) ENCODE zstd,
    rent_leasing_expense character varying(1) ENCODE zstd,
    advertising_expense character varying(1) ENCODE zstd,
    utilities_expense character varying(1) ENCODE zstd,
    accounting_expense character varying(1) ENCODE zstd,
    packagecontainer_expense character varying(1) ENCODE zstd,
    contract_labor_expense character varying(1) ENCODE zstd,
    insurance_expense character varying(1) ENCODE zstd,
    purchased_print_expense character varying(1) ENCODE zstd,
    purchase_mgmtadmin_svc_expense character varying(1) ENCODE zstd,
    legal_expense character varying(1) ENCODE zstd,
    ein character varying(9) ENCODE zstd,
    franchise_code_1 character varying(3) ENCODE zstd,
    franchise_code_2 character varying(3) ENCODE zstd,
    franchise_code_3 character varying(3) ENCODE zstd,
    franchise_code_4 character varying(3) ENCODE zstd,
    franchise_code_5 character varying(3) ENCODE zstd,
    franchise_code_6 character varying(3) ENCODE zstd,
    servicetype character varying(1) ENCODE zstd,
    addresstype character varying(2) ENCODE zstd,
    fulfillmentflag character varying(1) ENCODE zstd,
    affiliatedlocations character varying(6) ENCODE zstd,
    affiliatedrecords character varying(6) ENCODE zstd,
    lastname character varying(14) ENCODE zstd,
    firstname character varying(11) ENCODE zstd,
    salutation character varying(2) ENCODE zstd,
    proftitle character varying(3) ENCODE zstd,
    titlecode character varying(3) ENCODE zstd,
    gender character varying(1) ENCODE zstd,
    email_flag character varying(1) ENCODE zstd,
    vendorid2 character varying(2) ENCODE zstd,
    executivesourcecode character varying(2) ENCODE zstd,
    femaleexecflag character varying(1) ENCODE zstd,
    highincomeexecflag character varying(1) ENCODE zstd,
    vendor_ethnicity character varying(2) ENCODE zstd,
    vendor_ethnic_group character varying(1) ENCODE zstd,
    vendor_language character varying(2) ENCODE zstd,
    vendor_religion character varying(1) ENCODE zstd,
    short_primary_web_address character varying(256) ENCODE zstd,
    long_local_web_address character varying(256) ENCODE zstd,
    coupon_link character varying(384) ENCODE zstd,
    twitter_link character varying(384) ENCODE zstd,
    linked_in_link character varying(384) ENCODE zstd,
    facebook_link character varying(384) ENCODE zstd,
    alternate_social_link character varying(384) ENCODE zstd,
    you_tube_link character varying(384) ENCODE zstd,
    logo_link character varying(384) ENCODE zstd,
    googleplus_link character varying(384) ENCODE zstd,
    pinterest_link character varying(384) ENCODE zstd,
    myspace_link character varying(384) ENCODE zstd,
    foursquare character varying(384) ENCODE zstd,
    yelp character varying(384) ENCODE zstd,
    instagram_link character varying(384) ENCODE zstd,
    squarefootage8 character varying(1) ENCODE zstd,
    popcode character varying(1) ENCODE zstd,
    ecommercepresent character varying(1) ENCODE zstd,
    googecheckout character varying(1) ENCODE zstd,
    paypal character varying(1) ENCODE zstd,
    greenadopterscore character varying(1) ENCODE zstd,
    locationlinkageid character varying(9) ENCODE zstd,
    locationlinkagetype character varying(1) ENCODE zstd,
    fleetsize character varying(1) ENCODE zstd,
    charitable_dontations_expense character varying(1) ENCODE zstd,
    license_fees_taxes character varying(1) ENCODE zstd,
    maintenance_repair character varying(1) ENCODE zstd,
    nbrc_new_add_date character varying(8) ENCODE zstd,
    new_to_database_date character varying(8) ENCODE zstd,
    professional_sic_flag character varying(1) ENCODE zstd,
    last_verification_date_limited character varying(6) ENCODE zstd,
    contactid character varying(12) ENCODE zstd,
    transportationexpensecode character varying(1) ENCODE zstd,
    phone_number_type character varying(1) ENCODE zstd,
    contactquality character varying(1) ENCODE zstd,
    digitalmatch character varying(1) ENCODE zstd,
    delivery_date character varying(8) ENCODE zstd,
    functional_area_code character varying(4) ENCODE zstd,
    department_code character varying(2) ENCODE zstd,
    role_code character varying(4) ENCODE zstd,
    level_code character(1) ENCODE zstd,
    literaltitle_id character varying(8) ENCODE zstd,
    inferred_corporate_employee_count integer ENCODE az64,
    inferred_corporate_employee_range character(1) ENCODE zstd,
    inferred_corporate_sales_volume_amount integer ENCODE az64,
    inferred_corporate_sales_volume_range character(1) ENCODE zstd,
    inferred_corporate_employee_source_indicator character(1) ENCODE zstd,
    inferred_corporate_sales_volume_source_indicator character(1) ENCODE zstd,
    change_date character varying(8) ENCODE bytedict,
    transaction_date character varying(8) ENCODE zstd,
    legal_name character varying(120) ENCODE zstd,
    last_date character varying(6) ENCODE bytedict,
    typecode character(1) ENCODE zstd,
    contactsourcedate character varying(8) ENCODE zstd,
    districtcode character varying(5) ENCODE bytedict,
    preferredcorrespondence character(1) ENCODE zstd,
    creditratingcode character(1) ENCODE zstd,
    actualemployees integer ENCODE az64,
    actualsales integer ENCODE az64,
    addressoriginationdate character varying(6) ENCODE bytedict,
    businessnameoriginationdate character varying(6) ENCODE bytedict,
    databasesegmentationcode character(1) ENCODE zstd,
    grade character(1) ENCODE zstd,
    housenumber character varying(15) ENCODE zstd,
    locationemployeesize character(1) ENCODE zstd,
    locationname character varying(30) ENCODE zstd,
    locationsalesvolume character(1) ENCODE zstd,
    mailingdetailflag character(1) ENCODE zstd,
    mailscore character varying(2) ENCODE zstd,
    newtodatabasedate character varying(8) ENCODE zstd,
    phoneoriginationdate character varying(6) ENCODE bytedict,
    streetname character varying(28) ENCODE zstd,
    streetpostdirectional character varying(2) ENCODE zstd,
    streetpredirectional character varying(2) ENCODE zstd,
    streetsuffix character varying(5) ENCODE zstd,
    websiteflag character(1) ENCODE zstd,
    utilitysource character(1) ENCODE zstd,
    aclflag character(1) ENCODE zstd,
    amiflag character(1) ENCODE zstd,
    federalcontractorflag character(1) ENCODE zstd,
    changedtooutofbusinessdate character varying(8) ENCODE zstd,
    ami_currentlinkagenumber character varying(9) ENCODE zstd,
    ami_typeofpractice character varying(1) ENCODE zstd,
    ami_primaryspecialty character varying(3) ENCODE zstd,
    ami_secondaryspecialty character varying(3) ENCODE zstd,
    ami_boardcertifiedindicator character varying(1) ENCODE zstd,
    ami_dateofbirth character varying(6) ENCODE zstd,
    ami_yearofgraduation character varying(4) ENCODE zstd,
    ami_medicalschoolcode_0 character varying(5) ENCODE zstd,
    ami_officemanagerlastname character varying(14) ENCODE zstd,
    ami_officemanagerfirstname character varying(11) ENCODE zstd,
    ami_professionaldegreecode character varying(1) ENCODE zstd,
    ami_addresstype character varying(1) ENCODE zstd,
    ami_upin character varying(6) ENCODE zstd,
    ami_deanumber character varying(9) ENCODE zstd,
    ami_sizegrouppractice character varying(3) ENCODE zstd,
    ami_hospitalnumber character varying(7) ENCODE zstd,
    ami_numberofnurses character varying(5) ENCODE zstd,
    ami_nursepractitioners character varying(5) ENCODE zstd,
    ami_numberofchairs character varying(5) ENCODE zstd,
    ami_lic_stateoflicense character varying(2) ENCODE zstd,
    ami_lic_expirationdate character varying(8) ENCODE zstd,
    ami_lic_licenseboardtype character varying(3) ENCODE zstd,
    ami_lic_licensenumber character varying(30) ENCODE zstd,
    ami_networkcode character varying(3) ENCODE zstd,
    ami_associationcode character varying(3) ENCODE zstd,
    ami_npi character varying(10) ENCODE zstd,
    ami_prescriptionsperwk character varying(1) ENCODE zstd,
    ami_primarywebaddressflag character varying(1) ENCODE zstd,
    ami_activewebaddressflag character varying(1) ENCODE zstd,
    acl_groupcode character varying(2) ENCODE zstd,
    acl_denominationcode character varying(4) ENCODE zstd,
    acl_buyercode1 character varying(2) ENCODE zstd,
    acl_buyercode2 character varying(2) ENCODE zstd,
    acl_buyercode3 character varying(2) ENCODE zstd,
    acl_buyercode4 character varying(2) ENCODE zstd,
    acl_buyercode5 character varying(2) ENCODE zstd,
    acl_buyercode6 character varying(2) ENCODE zstd,
    acl_buyercode7 character varying(2) ENCODE zstd,
    acl_buyercode8 character varying(2) ENCODE zstd,
    acl_buyercode9 character varying(2) ENCODE zstd,
    acl_buyercode10 character varying(2) ENCODE zstd,
    acl_buyercode11 character varying(2) ENCODE zstd,
    acl_buyercode12 character varying(2) ENCODE zstd,
    acl_buyercode13 character varying(2) ENCODE zstd,
    acl_buyercode14 character varying(2) ENCODE zstd,
    acl_buyercode15 character varying(2) ENCODE zstd,
    acl_buyercode16 character varying(2) ENCODE zstd,
    acl_buyercode17 character varying(2) ENCODE zstd,
    acl_buyercode18 character varying(2) ENCODE zstd,
    acl_buyercode19 character varying(2) ENCODE zstd,
    acl_buyercode20 character varying(2) ENCODE zstd,
    acl_buyercode21 character varying(2) ENCODE zstd,
    acl_buyercode22 character varying(2) ENCODE zstd,
    acl_buyercode23 character varying(2) ENCODE zstd,
    acl_buyercode24 character varying(2) ENCODE zstd,
    acl_buyercode25 character varying(2) ENCODE zstd,
    acl_buyercode26 character varying(2) ENCODE zstd,
    acl_buyercode27 character varying(2) ENCODE zstd,
    acl_buyercode28 character varying(2) ENCODE zstd,
    acl_buyercode29 character varying(2) ENCODE zstd,
    acl_buyercode30 character varying(2) ENCODE zstd,
    acl_buyercode31 character varying(2) ENCODE zstd,
    acl_buyercode32 character varying(2) ENCODE zstd,
    acl_churchsizecode character varying(1) ENCODE zstd,
    acl_churchsize character varying(5) ENCODE zstd,
    acl_attendancecode character varying(1) ENCODE zstd,
    acl_attendance character varying(5) ENCODE zstd,
    acl_schoolsizecode character varying(1) ENCODE zstd,
    acl_raceofcongregation1 character varying(1) ENCODE zstd,
    acl_raceofcongregation2 character varying(1) ENCODE zstd,
    acl_raceofcongregation3 character varying(1) ENCODE zstd,
    acl_raceofcongregation4 character varying(1) ENCODE zstd,
    acl_raceofcongregation5 character varying(1) ENCODE zstd,
    acl_raceofcongregation6 character varying(1) ENCODE zstd,
    acl_membershipsourceflag character varying(1) ENCODE zstd,
    acl_multiplecongregationscode character varying(1) ENCODE zstd,
    acl_newconstructionlastverificationdate character varying(8) ENCODE zstd,
    countrycode character varying(2) ENCODE zstd,
    dmacode character varying(3) ENCODE bytedict,
    changedtoverifieddate character varying(8) ENCODE zstd,
    changedtosuspectdate character varying(8) ENCODE zstd,
    infoscore character varying(3) ENCODE bytedict,
    industrygrowthscore character varying(3) ENCODE bytedict,
    completenessscore character varying(3) ENCODE bytedict,
    stabilityscore character varying(3) ENCODE bytedict,
    reachabilityscore character varying(3) ENCODE bytedict,
    infograde character varying(2) ENCODE zstd,
    industrygrowthgrade character varying(2) ENCODE zstd,
    completenessgrade character varying(2) ENCODE zstd,
    stabilitygrade character varying(2) ENCODE zstd,
    reachabilitygrade character varying(2) ENCODE zstd,
    ami_boardcertificationcount character varying(5) ENCODE zstd,
    ami_medicareaccepted character varying(1) ENCODE zstd,
    ami_acceptingnewpatients character varying(1) ENCODE zstd,
    namestandardizationflag character varying(1) ENCODE zstd,
    timezone character varying(3) ENCODE zstd,
    homeaddress character varying(30) ENCODE zstd,
    homecity character varying(30) ENCODE zstd,
    homestate character varying(2) ENCODE zstd,
    homezipcode character varying(5) ENCODE zstd,
    homezip4 character varying(4) ENCODE zstd,
    homeareacode character varying(3) ENCODE zstd,
    homephonenumber character varying(7) ENCODE zstd,
    donotcallflag character varying(1) ENCODE zstd,
    homecountycode character varying(3) ENCODE zstd,
    homelocationtype character varying(1) ENCODE zstd,
    execgender character varying(1) ENCODE zstd,
    execage character varying(2) ENCODE zstd,
    execincome character varying(1) ENCODE zstd,
    homeowner character varying(1) ENCODE zstd,
    maritalstatus character varying(1) ENCODE zstd,
    job_function_id character varying(3) ENCODE zstd,
    management_level character varying(25) ENCODE zstd,
    job_title character varying(80) ENCODE zstd,
    employeesizecode1 character varying(1) ENCODE zstd,
    outputvolume1 character varying(9) ENCODE zstd,
    outputvolumecode1 character varying(1) ENCODE zstd,
    bankassetcode character varying(1) ENCODE zstd,
    corporateemployees character varying(6) ENCODE zstd,
    corporateoutputvolume character varying(9) ENCODE zstd,
    corporatesalesvolume character(1) ENCODE zstd,
    flatitude_old double precision ENCODE raw,
    flongitude_old double precision ENCODE raw,
    fullname character varying(40) ENCODE zstd,
    one_per_abi character varying(14) ENCODE zstd,
    one_per_abiname character varying(40) ENCODE zstd,
    name_available character varying(1) ENCODE zstd,
    company_available character varying(1) ENCODE zstd,
    abi_available character varying(1) ENCODE zstd,
    phone_number_available character varying(1) ENCODE zstd,
    city_available character varying(1) ENCODE zstd,
    website_available character varying(1) ENCODE zstd,
    ein_available character varying(1) ENCODE zstd,
    one_per_contact_name character varying(40) ENCODE zstd,
    one_per_company_name_title character varying(35) ENCODE zstd,
    contacts_per_company character varying(60) ENCODE zstd,
    zipradius character(1) ENCODE zstd,
    georadius character(1) ENCODE zstd,
    one_per_address_company character varying(60) ENCODE zstd,
    presence_of_latitude character(1) ENCODE zstd,
    presence_of_longitude character(1) ENCODE zstd,
    creditcardsaccepted character varying(15) ENCODE zstd,
    creditcardsaccepted_multi character varying(50) ENCODE zstd,
    parentabinumber_back character varying(9) ENCODE zstd,
    haspostal character(1) ENCODE zstd,
    listid integer ENCODE az64,
    permissiontype character varying(1) ENCODE zstd,
    vendor_id character varying(2) ENCODE zstd,
    geo_match_level character(1) ENCODE zstd,
    database_flag character(1) ENCODE zstd,
    work_at_home_indicator character(1) ENCODE zstd,
    titlecode_back character varying(3) ENCODE zstd,
    acl_buyercode character varying(121) ENCODE zstd,
    emailaddress character varying(60) ENCODE zstd,
    cdomain character varying(80) ENCODE zstd,
    cdomaintype character varying(2) ENCODE zstd,
    bademails character(1) ENCODE zstd,
    royalty_emails character(1) ENCODE zstd,
    infogroup_emails character(1) ENCODE zstd,
    cinclude character(1) ENCODE zstd,
    md5upper character varying(32) ENCODE zstd,
    md5lower character varying(32) ENCODE zstd,
    md5saltedhash character varying(32) ENCODE zstd,
    sha256lower character varying(64) ENCODE zstd,
    sha256upper character varying(64) ENCODE zstd,
    bvt_email_status_email character(1) ENCODE zstd,
    bridge_code character(1) ENCODE zstd,
    domain_id character(1) ENCODE zstd,
    topleveldomain character(6) ENCODE zstd,
    email_marketable character varying(1) ENCODE zstd,
    email_deliverable character varying(1) ENCODE zstd,
    email_reputation_risk character varying(1) ENCODE zstd,
    department_code_email character varying(4) ENCODE zstd,
    department_code_multi_email character varying(9) ENCODE zstd,
    franchise_code_email character varying(6) ENCODE zstd,
    individual_firm_email character(1) ENCODE zstd,
    job_level_code_email character varying(4) ENCODE zstd,
    job_level_code_multi_email character varying(9) ENCODE zstd,
    mail_score_email character varying(2) ENCODE zstd,
    oneperflag_email character(1) ENCODE zstd,
    one_per_abi_email_email character varying(69) ENCODE zstd,
    one_per_company_email character varying(30) ENCODE zstd,
    one_per__abi_email character(1) ENCODE zstd,
    productcode_email character(1) ENCODE zstd,
    responder_date_email character varying(6) ENCODE zstd,
    seq_email character varying(9) ENCODE zstd,
    suppression_type_email character(1) ENCODE zstd,
    title_description_email character varying(150) ENCODE zstd,
    title_rank_email character varying(2) ENCODE zstd,
    transaction_date_email character varying(6) ENCODE zstd,
    transaction_type_email character(1) ENCODE zstd,
    ultimate_number_email character varying(9) ENCODE zstd,
    year_sic_added_email character varying(6) ENCODE zstd,
    additionalemailflag_email character varying(1) ENCODE zstd,
    scf character varying(5) ENCODE zstd,
    zip_end character varying(5) ENCODE zstd,
    application_a character varying(1) ENCODE zstd,
    application_b character varying(1) ENCODE zstd,
    application_c character varying(1) ENCODE zstd,
    application_e character varying(1) ENCODE zstd,
    application_f character varying(1) ENCODE zstd,
    application_g character varying(1) ENCODE zstd,
    application_h character varying(1) ENCODE zstd,
    application_i character varying(1) ENCODE zstd,
    application_j character varying(1) ENCODE zstd,
    application_k character varying(1) ENCODE zstd,
    application_l character varying(1) ENCODE zstd,
    application_m character varying(1) ENCODE zstd,
    application_n character varying(1) ENCODE zstd,
    application_p character varying(1) ENCODE zstd,
    segmentcode_1 character varying(1) ENCODE zstd,
    segmentcode_2 character varying(1) ENCODE zstd,
    segmentcode_3 character varying(1) ENCODE zstd,
    segmentcode_4 character varying(1) ENCODE zstd,
    segmentcode_5 character varying(1) ENCODE zstd,
    segmentcode_6 character varying(1) ENCODE zstd,
    segmentcode_7 character varying(1) ENCODE zstd,
    segmentcode_8 character varying(1) ENCODE zstd,
    segmentcode_9 character varying(1) ENCODE zstd,
    segmentcode_a character varying(1) ENCODE zstd,
    segmentcode_f character varying(1) ENCODE zstd,
    segmentcode_h character varying(1) ENCODE zstd,
    segmentcode_i character varying(1) ENCODE zstd,
    nbrc_new_add_date_yyyymm character varying(8) ENCODE zstd,
    new_to_database_date_yyyymm character varying(8) ENCODE zstd,
    transaction_date_yyyymm character varying(8) ENCODE bytedict,
    areacode character varying(10) ENCODE bytedict,
    faxavailable character varying(1) ENCODE zstd,
    statecountycode character varying(5) ENCODE zstd,
    zipcarrierroute character varying(9) ENCODE zstd,
    primarysic6franchise character varying(9) ENCODE zstd,
    kiosks_flag character varying(1) ENCODE zstd,
    hardkey character varying(53) ENCODE zstd,
    pobox_flag character varying(1) ENCODE zstd,
    parksuppression character(1) ENCODE zstd,
    addressline1 character varying(30) ENCODE zstd,
    addressline2 character varying(30) ENCODE zstd,
    areacode_prefix character varying(10) ENCODE zstd,
    one_per_address character varying(39) ENCODE zstd,
    infogroupcontact character varying(1) ENCODE zstd,
    infogroupcontactandemail character varying(1) ENCODE zstd,
    mailscorecombined character varying(1) ENCODE zstd,
    census_state_code_2010 character varying(2) ENCODE bytedict,
    statecode character varying(2) ENCODE bytedict,
    mailing_statecode character varying(2) ENCODE bytedict,
    countycodebystatecode character varying(5) ENCODE zstd,
    citybystate character varying(18) ENCODE zstd,
    state_county_censustract_block character varying(12) ENCODE zstd,
    naics2 character varying(8) ENCODE zstd,
    naics4 character varying(8) ENCODE bytedict,
    naics6 character varying(8) ENCODE zstd,
    usterritoryflag character varying(1) ENCODE zstd,
    statecountyname character varying(77) ENCODE zstd,
    statecityzip character varying(31) ENCODE zstd,
    statecityscf character varying(31) ENCODE zstd,
    uniqueprofessionalgui character varying(10) ENCODE zstd,
    titlecodepriority integer ENCODE az64,
    ami_hospitalname character varying(27) ENCODE zstd,
    locationzip_zip4_deliverypointbarcode character varying(12) ENCODE zstd,
    state_description character varying(100) ENCODE zstd,
    ethnicity_description character varying(100) ENCODE zstd,
    stockexchange_description character varying(100) ENCODE zstd,
    phonecallstatus_description character varying(100) ENCODE zstd,
    businesscreditalphascore_description character varying(100) ENCODE zstd,
    fleetsize_description character varying(100) ENCODE zstd,
    franchise_code_1_description character varying(100) ENCODE zstd,
    franchise_code_2_description character varying(100) ENCODE zstd,
    franchise_code_3_description character varying(100) ENCODE zstd,
    franchise_code_4_description character varying(100) ENCODE zstd,
    franchise_code_5_description character varying(100) ENCODE zstd,
    franchise_code_6_description character varying(100) ENCODE zstd,
    accounting_expense_description character varying(100) ENCODE zstd,
    advertising_expense_description character varying(100) ENCODE zstd,
    charitable_dontations_expense_description character varying(100) ENCODE zstd,
    contract_labor_expense_description character varying(100) ENCODE zstd,
    insurance_expense_description character varying(100) ENCODE zstd,
    legal_expense_description character varying(100) ENCODE zstd,
    license_fees_taxes_description character varying(100) ENCODE zstd,
    maintenance_repair_description character varying(100) ENCODE zstd,
    office_equipment_expense_description character varying(100) ENCODE zstd,
    packagecontainer_expense_description character varying(100) ENCODE zstd,
    payroll_expense_description character varying(100) ENCODE zstd,
    purchased_print_expense_description character varying(100) ENCODE zstd,
    purchase_mgmtadmin_svc_expense_description character varying(100) ENCODE zstd,
    rent_leasing_expense_description character varying(100) ENCODE zstd,
    technology_expense_description character varying(100) ENCODE zstd,
    telecommunications_expense_description character varying(100) ENCODE zstd,
    transportationexpensecode_description character varying(100) ENCODE zstd,
    utilities_expense_description character varying(100) ENCODE zstd,
    salutation_description character varying(100) ENCODE zstd,
    acl_groupcode_description character varying(100) ENCODE zstd,
    acl_denominationcode_description character varying(100) ENCODE zstd,
    dmacode_description character varying(100) ENCODE zstd,
    proftitle_description character varying(100) ENCODE zstd,
    addresstype_description character varying(100) ENCODE zstd,
    ownorlease_description character varying(100) ENCODE zstd,
    ami_boardcertifiedindicator_description character varying(100) ENCODE zstd,
    ami_primaryspecialty_description character varying(100) ENCODE zstd,
    ami_secondaryspecialty_description character varying(100) ENCODE zstd,
    ami_professionaldegreecode_description character varying(100) ENCODE zstd,
    ami_lic_stateoflicense_description character varying(100) ENCODE zstd,
    ami_prescriptionsperwk_description character varying(100) ENCODE zstd,
    creditratingcode_description character varying(100) ENCODE zstd,
    phone_number_type_description character varying(100) ENCODE zstd,
    primarysic2_desc character varying(100) ENCODE zstd,
    primarysic4_desc character varying(100) ENCODE zstd,
    industry_desc character varying(100) ENCODE zstd,
    preferredcity character varying(50) ENCODE zstd,
    regioncode_multi character varying(60) ENCODE zstd,
    amexsflag character varying(1) ENCODE zstd,
    individual_mc character varying(17) ENCODE zstd,
    company_mc character varying(15) ENCODE zstd,
    address_mc character varying(17) ENCODE zstd
)
DISTSTYLE KEY;


COPY {maintable_name}
FROM 's3://idms-7933-prod/temp/business-csv/full'
IAM_ROLE '{iam}'
CSV
GZIP
TRIMBLANKS
DELIMITER '|'
;