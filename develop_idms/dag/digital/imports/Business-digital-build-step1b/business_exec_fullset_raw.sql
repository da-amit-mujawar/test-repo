DROP TABLE IF EXISTS digital.business_exec_fullset_raw;
CREATE TABLE IF NOT EXISTS digital.business_exec_fullset_raw
             (
                          id_in                         BIGINT  identity,
                          abinumber                     VARCHAR( 9 ),
                          executivedetailflag           VARCHAR( 1 ),
                          e_femaleexecflag              VARCHAR( 1 ),
                          e_firstname                   VARCHAR( 11 ),
                          e_gender                      VARCHAR( 1 ),
                          e_highincomeexecflag          VARCHAR( 1 ),
                          e_lastname                    VARCHAR( 14 ),
                          e_proftitle                   VARCHAR( 6 ),
                          e_executivesourcecode         VARCHAR( 2 ),
                          e_salutationcode              VARCHAR( 3 ),
                          e_titlecode                   VARCHAR( 1 ),
                          e_literaltitle                VARCHAR( 8 ),
                          e_typecode                    VARCHAR( 1 ),
                          e_vendorethnicgroup           VARCHAR( 3 ),
                          e_vendorethnicity             VARCHAR( 2 ),
                          e_vendorlanguage              VARCHAR( 3 ),
                          e_vendorreligion              VARCHAR( 1 ),
                          e_countryoforigin             VARCHAR( 3 ),
                          e_emailflag                   VARCHAR( 1 ),
                          e_emailaddress                VARCHAR( 60 ),
                          e_vendorid2                   VARCHAR( 2 ),
                          email_suppress                VARCHAR( 1 ),
                          e_bvt_email_status            VARCHAR( 1 ),
                          e_areacode_v1                 VARCHAR( 3 ),
                          e_phonenumber_v1              VARCHAR( 7 ),
                          amiexec_abinumber             VARCHAR( 9 ),
                          amiexec_age                   VARCHAR( 2 ),
                          amiexec_amicreditcardpresence VARCHAR( 1 ),
                          amiexec_execgender            VARCHAR( 1 ),
                          amiexec_homeaddress           VARCHAR( 30 ),
                          amiexec_homecity              VARCHAR( 18 ),
                          amiexec_homestate             VARCHAR( 2 ),
                          amiexec_homezipcode           VARCHAR( 5 ),
                          amiexec_homezipfour           VARCHAR( 4 ),
                          amiexec_homecountycode        VARCHAR( 3 ),
                          amiexec_homeowner             VARCHAR( 1 ),
                          amiexec_incomecode            VARCHAR( 1 ),
                          amiexec_locationtype          VARCHAR( 1 ),
                          amiexec_maritalstatus         VARCHAR( 1 ),
                          amiexec_homeareacode          VARCHAR( 3 ),
                          amiexec_homephonenumber       VARCHAR( 7 ),
                          amiexec_nonsolicitationflag   VARCHAR( 1 ),
                          e_contactid                   VARCHAR( 12 ),
                          e_execage                     VARCHAR( 2 ),
                          e_creditcardpresence          VARCHAR( 1 ),
                          e_execgender                  VARCHAR( 1 ),
                          e_homeaddress                 VARCHAR( 30 ),
                          e_homecity                    VARCHAR( 30 ),
                          e_homestate                   VARCHAR( 2 ),
                          e_homezipcode                 VARCHAR( 5 ),
                          e_homezip4                    VARCHAR( 4 ),
                          e_homecountycode              VARCHAR( 3 ),
                          e_homeowner                   VARCHAR( 1 ),
                          e_execincome                  VARCHAR( 1 ),
                          e_homelocationtype            VARCHAR( 1 ),
                          e_maritalstatus               VARCHAR( 1 ),
                          e_homeareacode                VARCHAR( 3 ),
                          e_homephonenumber             VARCHAR( 7 ),
                          e_donotcallflag               VARCHAR( 1 ),
                          e_homelatitude                VARCHAR( 9 ),
                          e_homelongitude               VARCHAR( 9 ),
                          e_homematchcode               VARCHAR( 1 ),
                          e_individualid                VARCHAR( 12 ),
                          e_familyid                    VARCHAR( 12 ),
                          e_sesi                        VARCHAR( 2 ),
                          e_homevaluecode               VARCHAR( 1 ),
                          e_emailaddressmd5upper        VARCHAR( 32 ),
                          e_emailaddressmd5lower        VARCHAR( 32 ),
                          e_departmentcode              VARCHAR( 32 ),
                          e_functionalareacode          VARCHAR( 32 ),
                          e_levelcode                   VARCHAR( 32 ),
                          e_rolecode                    VARCHAR( 32 )
             );
             
COPY digital.business_exec_fullset_raw
    FROM 's3://digital-7933-business/input/Business_data/executive.gz'
    iam_role {iam}
    GZIP
    REMOVEQUOTES
    IGNOREBLANKLINES
    ACCEPTINVCHARS
    MAXERROR 100
    DELIMITER ',';
