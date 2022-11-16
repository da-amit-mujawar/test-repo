drop table if exists tblchild6_{build_id}_{build} CASCADE;

create table tblchild6_{build_id}_{build}
(
cid varchar(18) ENCODE RAW,
Profile_fin_indid varchar(12) ENCODE ZSTD,
Profile_fin_hhld varchar(12) ENCODE ZSTD,
Profile_TOT_DATE_Code varchar(1) ENCODE ZSTD,
Profile_TOT_dlrs_12m_Code varchar(1) ENCODE ZSTD,
Profile_TOT_trns_12m_Code varchar(1) ENCODE ZSTD,
Profile_cat_DATE_Code varchar(1) ENCODE ZSTD,
Profile_cat_dlrs_12m_Code varchar(1) ENCODE ZSTD,
Profile_cat_trns_12m_Code varchar(1) ENCODE ZSTD,
Profile_rtl_DATE_Code varchar(1) ENCODE ZSTD,
Profile_rtl_dlrs_12m_Code varchar(1) ENCODE ZSTD,
Profile_rtl_trns_12m_Code varchar(1) ENCODE ZSTD,
Profile_web_DATE_Code varchar(1) ENCODE ZSTD,
Profile_web_dlrs_12m_Code varchar(1) ENCODE ZSTD,
Profile_web_trns_12m_Code varchar(1) ENCODE ZSTD,
Profile_cctype_12m_visa varchar(1) ENCODE ZSTD,
Profile_cctype_12m_mc varchar(1) ENCODE ZSTD,
Profile_cctype_12m_amex varchar(1) ENCODE ZSTD,
Profile_cctype_12m_disc varchar(1) ENCODE ZSTD,
Profile_cctype_12m_btq varchar(1) ENCODE ZSTD,
Profile_cctype_12m_oth varchar(1) ENCODE ZSTD,
Profile_cctype_12m_ukn varchar(1) ENCODE ZSTD,
Profile_cctype_13p_visa varchar(1) ENCODE ZSTD,
Profile_cctype_13p_mc varchar(1) ENCODE ZSTD,
Profile_cctype_13p_amex varchar(1) ENCODE ZSTD,
Profile_cctype_13p_disc varchar(1) ENCODE ZSTD,
Profile_cctype_13p_btq varchar(1) ENCODE ZSTD,
Profile_cctype_13p_oth varchar(1) ENCODE ZSTD,
Profile_cctype_13p_ukn varchar(1) ENCODE ZSTD,
Profile_pay_12m_db varchar(1) ENCODE ZSTD,
Profile_pay_12m_cc varchar(1) ENCODE ZSTD,
Profile_pay_12m_dc varchar(1) ENCODE ZSTD,
Profile_pay_12m_pc varchar(1) ENCODE ZSTD,
Profile_pay_12m_cash varchar(1) ENCODE ZSTD,
Profile_pay_12m_oth varchar(1) ENCODE ZSTD,
Profile_pay_12m_ukn varchar(1) ENCODE ZSTD,
Profile_pay_13p_db varchar(1) ENCODE ZSTD,
Profile_pay_13p_cc varchar(1) ENCODE ZSTD,
Profile_pay_13p_dc varchar(1) ENCODE ZSTD,
Profile_pay_13p_pc varchar(1) ENCODE ZSTD,
Profile_pay_13p_cash varchar(1) ENCODE ZSTD,
Profile_pay_13p_oth varchar(1) ENCODE ZSTD,
Profile_pay_13p_ukn varchar(1) ENCODE ZSTD,
Profile_channeltype_12m varchar(1) ENCODE ZSTD,
Profile_sc01_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc01_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc02_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc02_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc03_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc03_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc04_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc04_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc05_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc05_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc06_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc06_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc07_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc07_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc08_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc08_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc09_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc09_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc10_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc10_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc11_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc11_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc12_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc12_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc14_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc14_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc15_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc15_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc16_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc16_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc18_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc18_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc19_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc19_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc20_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc20_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc21_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc21_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_sc22_dlrs_12m_code varchar(1) ENCODE ZSTD,
Profile_sc22_trns_12m_code varchar(1) ENCODE ZSTD,
Profile_match_type varchar(1) ENCODE ZSTD
)
DISTSTYLE KEY
DISTKEY(cid)
SORTKEY(cid);


copy tblchild6_{build_id}_{build}
(cid,
profile_fin_indid,
profile_fin_hhld,
profile_tot_date_code,
profile_tot_dlrs_12m_code,
profile_tot_trns_12m_code,
profile_cat_date_code,
profile_cat_dlrs_12m_code,
profile_cat_trns_12m_code,
profile_rtl_date_code,
profile_rtl_dlrs_12m_code,
profile_rtl_trns_12m_code,
profile_web_date_code,
profile_web_dlrs_12m_code,
profile_web_trns_12m_code,
profile_cctype_12m_visa,
profile_cctype_12m_mc,
profile_cctype_12m_amex,
profile_cctype_12m_disc,
profile_cctype_12m_btq,
profile_cctype_12m_oth,
profile_cctype_12m_ukn,
profile_cctype_13p_visa,
profile_cctype_13p_mc,
profile_cctype_13p_amex,
profile_cctype_13p_disc,
profile_cctype_13p_btq,
profile_cctype_13p_oth,
profile_cctype_13p_ukn,
profile_pay_12m_db,
profile_pay_12m_cc,
profile_pay_12m_dc,
profile_pay_12m_pc,
profile_pay_12m_cash,
profile_pay_12m_oth,
profile_pay_12m_ukn,
profile_pay_13p_db,
profile_pay_13p_cc,
profile_pay_13p_dc,
profile_pay_13p_pc,
profile_pay_13p_cash,
profile_pay_13p_oth,
profile_pay_13p_ukn,
profile_channeltype_12m,
profile_sc01_dlrs_12m_code,
profile_sc01_trns_12m_code,
profile_sc02_dlrs_12m_code,
profile_sc02_trns_12m_code,
profile_sc03_dlrs_12m_code,
profile_sc03_trns_12m_code,
profile_sc04_dlrs_12m_code,
profile_sc04_trns_12m_code,
profile_sc05_dlrs_12m_code,
profile_sc05_trns_12m_code,
profile_sc06_dlrs_12m_code,
profile_sc06_trns_12m_code,
profile_sc07_dlrs_12m_code,
profile_sc07_trns_12m_code,
profile_sc08_dlrs_12m_code,
profile_sc08_trns_12m_code,
profile_sc09_dlrs_12m_code,
profile_sc09_trns_12m_code,
profile_sc10_dlrs_12m_code,
profile_sc10_trns_12m_code,
profile_sc11_dlrs_12m_code,
profile_sc11_trns_12m_code,
profile_sc12_dlrs_12m_code,
profile_sc12_trns_12m_code,
profile_sc14_dlrs_12m_code,
profile_sc14_trns_12m_code,
profile_sc15_dlrs_12m_code,
profile_sc15_trns_12m_code,
profile_sc16_dlrs_12m_code,
profile_sc16_trns_12m_code,
profile_sc18_dlrs_12m_code,
profile_sc18_trns_12m_code,
profile_sc19_dlrs_12m_code,
profile_sc19_trns_12m_code,
profile_sc20_dlrs_12m_code,
profile_sc20_trns_12m_code,
profile_sc21_dlrs_12m_code,
profile_sc21_trns_12m_code,
profile_sc22_dlrs_12m_code,
profile_sc22_trns_12m_code,
profile_match_type
)
from 's3://{s3-internal}/neptune/mGen/MGen_profile.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from tblchild6_{build_id}_{build}')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-profile/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
