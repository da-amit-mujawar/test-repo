DROP TABLE IF EXISTS digital.Consumer_Email_Fullset_raw;
create table if not exists digital.Consumer_Email_Fullset_raw
(
    acquisition_date         varchar(6),
    vendorid2                varchar(2),
    usage_indicator          varchar(1),
    firstname                varchar(15),
    middleinitial            varchar(1),
    lastname                 varchar(20),
    gender                   varchar(1),
    housenumber              varchar(10),
    streetpredirectional     varchar(2),
    streetname               varchar(28),
    streetsuffix             varchar(4),
    streetpostdirectional    varchar(2),
    unittype                 varchar(4),
    unitnumber               varchar(8),
    city                     varchar(28),
    state                    varchar(2),
    zipcode                  varchar(6),
    zipfour                  varchar(4),
    move_ind                 varchar(1),
    emailaddress             varchar(80),
    responderdate            varchar(6),
    suppression_type         varchar(1),
    familyid                 varchar(12),
    individualid             varchar(12),
    match_ind                varchar(1),
    mailconfidence           varchar(2),
    recordtype               varchar(1),
    alsincome                varchar(1),
    alsagecode               varchar(1),
    alslengthofresidence     varchar(2),
    alspurchasingpowerincome varchar(1),
    alshomevalue             varchar(1),
    alswealthfinder          varchar(1),
    psfducode                varchar(1),
    p10ducode                varchar(1),
    pownrocccode             varchar(1),
    ip_address               varchar(15),
    optin_date               varchar(8),
    url                      varchar(60),
    statecode                varchar(2),
    censuscountycode         varchar(3),
    censustract              varchar(6),
    censusblockgroup         varchar(1),
    matchcode                varchar(1),
    editedaddress            varchar(40),
    emaildatabase_extent     varchar(2),
    oldmasterindicator       varchar(1),
    prioritysourcecode       varchar(2),
    roadrunner_flag          varchar(1),
    source_counter           varchar(2),
    source_code_indicator1   varchar(10),
    source_code_indicator2   varchar(10),
    source_code_indicator3   varchar(10),
    latitude                 varchar(9),
    longitude                varchar(9),
    contactid                varchar(12),
    gst_sourcecode_indicator varchar(10),
    gst_source_counter       varchar(2),
    lemsmatchcode            varchar(18),
    reject_reason            varchar(3),
    dma_code                 varchar(3),
    bvt_email_status         varchar(1),
    bvt_refresh_date         varchar(8),
    ipst_validity_score      varchar(1),
    ipst_status_code         varchar(1),
    ipst_refresh_date        varchar(8),
    category                 varchar(36),
    email_clickthru_date     varchar(8),
    email_open_date          varchar(8),
    domain                   varchar(80),
    top_level_domain         varchar(6),
    dob_individual           varchar(8),
    home_owner               varchar(1),
    best_date                varchar(8),
    emaildb_flag             varchar(1),
    countrycode              varchar(2),
    emailaddress_md5upper    varchar(32),
    emailaddress_md5lower    varchar(32),
    id_in                    bigint identity
);

COPY digital.Consumer_Email_Fullset_raw
    FROM 's3://digital-7933-business/input/Consumer_email/'
    IAM_ROLE '{iam}'
    GZIP
    IGNOREBLANKLINES
    ACCEPTINVCHARS
    MAXERROR 100
    FIXEDWIDTH
    'Acquisition_Date:6,
    VendorID2:2,
    Usage_Indicator:1,
    Firstname:15,
    MiddleInitial:1,
    Lastname:20,
    Gender:1,
    Housenumber:10,
    Streetpredirectional:2,
    Streetname:28,
    Streetsuffix:4,
    Streetpostdirectional:2,
    UnitType:4,
    Unitnumber:8,
    City:28,
    State:2,
    Zipcode:6,
    Zipfour:4,
    Move_Ind:1,
    Emailaddress:80,
    Responderdate:6,
    Suppression_type:1,
    FamilyId:12,
    IndividualId:12,
    Match_Ind:1,
    Mailconfidence:2,
    Recordtype:1,
    Alsincome:1,
    Alsagecode:1,
    Alslengthofresidence:2,
    Alspurchasingpowerincome:1,
    Alshomevalue:1,
    Alswealthfinder:1,
    psfducode:1,
    p10ducode:1,
    pownrocccode:1,
    IP_Address:15,
    Optin_Date:8,
    Url:60,
    StateCode:2,
    CensusCountyCode:3,
    CensusTract:6,
    CensusBlockGroup:1,
    MatchCode:1,
    Editedaddress:40,
    EmailDatabase_Extent:2,
    OldMasterIndicator:1,
    PrioritySourceCode:2,
    RoadRunner_Flag:1,
    Source_Counter:2,
    Source_Code_Indicator1:10,
    Source_Code_Indicator2:10,
    Source_Code_Indicator3:10,
    Latitude:9,
    Longitude:9,
    ContactID:12,
    GST_SourceCode_Indicator:10,
    GST_Source_Counter:2,
    LemsMatchCode:18,
    Reject_Reason:3,
    DMA_Code:3,
    BVT_Email_Status:1,
    BVT_Refresh_Date:8,
    IPST_Validity_Score:1,
    IPST_Status_Code:1,
    IPST_Refresh_Date:8,
    Category:36,
    Email_Clickthru_Date:8,
    Email_Open_Date:8,
    Domain:80,
    Top_Level_Domain:6,
    DOB_Individual:8,
    Home_Owner:1,
    Best_Date:8,
    Emaildb_Flag:1,
    CountryCode:2,
    emailaddress.md5upper:32,
    emailaddress.md5lower:32';