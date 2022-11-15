DROP TABLE IF EXISTS digital.business_sic_fullset_qa;
CREATE TABLE IF NOT EXISTS digital.business_sic_fullset_qa
             (
                          abinumber                   VARCHAR(9),
                          s_actioncode1               VARCHAR(1),
                          s_actioncode2               VARCHAR(1),
                          s_adsizecode                VARCHAR(1),
                          s_attorneycuisinecode       VARCHAR(1),
                          s_bankassetcode             VARCHAR(1),
                          s_booknumber                VARCHAR(5),
                          s_dateindatabase            VARCHAR(6),
                          s_franchisecode1            VARCHAR(1),
                          s_franchisecode1_extended   VARCHAR(3),
                          s_franchisecode2            VARCHAR(1),
                          s_franchisecode2_extended   VARCHAR(3),
                          s_franchisecode3            VARCHAR(1),
                          s_franchisecode3_extended   VARCHAR(3),
                          s_franchisecode4            VARCHAR(1),
                          s_franchisecode4_extended   VARCHAR(3),
                          s_franchisecode5            VARCHAR(1),
                          s_franchisecode5_extended   VARCHAR(3),
                          s_franchisecode6            VARCHAR(1),
                          s_franchisecode6_extended   VARCHAR(3),
                          s_industryspecificfirstbyte VARCHAR(1),
                          s_naics6                    VARCHAR(6),
                          s_newadddate                VARCHAR(6),
                          s_primarysicdesignator      VARCHAR(1),
                          s_professionalsicflag       VARCHAR(1),
                          s_siccode_altbase           VARCHAR(8),
                          s_siccode                   VARCHAR(6),
                          s_sicyearfirstappeared      VARCHAR(4),
                          s_truefranchiseflag         VARCHAR(1),
                          s_updatedate                VARCHAR(6),
                          s_yellowpagecode            VARCHAR(5),
                          siccount                    VARCHAR(6),
                          s_pnaics2                   VARCHAR(2),
                          s_naics2                    VARCHAR(2),
                          s_pnaics4                   VARCHAR(4),
                          s_naics4                    VARCHAR(4),
                          s_bookpublicationdate       VARCHAR(6),
                          s_frenchflag                VARCHAR(1)
             );


COPY digital.business_sic_fullset_qa
    FROM 's3://digital-7933-business/input/Business_data/sic.gz'
    iam_role {iam}
    REMOVEQUOTES
    GZIP
    IGNOREBLANKLINES
    ACCEPTINVCHARS
    MAXERROR 100
    DELIMITER ',';
