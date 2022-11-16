/*
 Create Child2 mGen Apogee Table.
 Source is Flat File exported from mGen Build Process (TBD - Insert DAG Name Here??)
 --       nvl(a.lems, '') alems,
removed duplicate field
 Update Schedule: Monthly
*/


--exclude_tblmgenapogee_new
drop table if exists exclude_nonprofit_child2_{dbid};
create table exclude_nonprofit_child2_{dbid}
(
    last_transaction_overall_days integer       encode az64,
    tot_parts                     integer       encode az64,
    tot_dlrs_12m                  integer       encode az64,
    tot_trns_12m                  integer       encode az64,
    tot_dlrs_lt                   integer       encode az64,
    tot_trns_lt                   integer       encode az64,
    tot_dlrs_12m_avg              integer       encode az64,
    tot_dlrs_lt_avg               integer       encode az64,
    last_transaction_catalog_days integer       encode az64,
    cat_part                      integer       encode az64,
    cat_dlrs_12m                  integer       encode az64,
    cat_trns_12m                  integer       encode az64,
    cat_dlrs_lt                   integer       encode az64,
    cat_trns_lt                   integer       encode az64,
    cat_dlrs_12m_avg              integer       encode az64,
    cat_dlrs_lt_avg               integer       encode az64,
    last_transaction_web_days     integer       encode az64,
    web_parts                     integer       encode az64,
    web_dlrs_12m                  integer       encode az64,
    web_trns_12m                  integer       encode az64,
    web_dlrs_lt                   integer       encode az64,
    web_trns_lt                   integer       encode az64,
    web_dlrs_12m_avg              integer       encode az64,
    web_dlrs_lt_avg               integer       encode az64,
    last_transaction_retail_days  integer       encode az64,
    rtl_parts                     integer       encode az64,
    rtl_dlrs_12m                  integer       encode az64,
    rtl_trns_12m                  integer       encode az64,
    rtl_dlrs_lt                   integer       encode az64,
    rtl_trns_lt                   integer       encode az64,
    rtl_dlrs_12m_avg              integer       encode az64,
    rtl_dlrs_lt_avg               integer       encode az64,
    nextaction_id                 varchar(60)   encode zstd,
    lems                          varchar(18)   encode zstd,
    paid_12month_pc               char(1)       encode zstd,
    paid_12month_cc               char(1)       encode zstd,
    paid_12month_cash             char(1)       encode zstd,
    company_id                    varchar(25)   ,
    filler                        varchar(2)
)
    diststyle key
    distkey (company_id)
    sortkey (company_id)
;

copy exclude_nonprofit_child2_{dbid}
    (
     last_transaction_overall_days,
     tot_parts,
     tot_dlrs_12m,
     tot_trns_12m,
     tot_dlrs_lt,
     tot_trns_lt,
     tot_dlrs_12m_avg,
     tot_dlrs_lt_avg,
     last_transaction_catalog_days,
     cat_part,
     cat_dlrs_12m,
     cat_trns_12m,
     cat_dlrs_lt,
     cat_trns_lt,
     cat_dlrs_12m_avg,
     cat_dlrs_lt_avg,
     last_transaction_web_days,
     web_parts,
     web_dlrs_12m,
     web_trns_12m,
     web_dlrs_lt,
     web_trns_lt,
     web_dlrs_12m_avg,
     web_dlrs_lt_avg,
     last_transaction_retail_days,
     rtl_parts,
     rtl_dlrs_12m,
     rtl_trns_12m,
     rtl_dlrs_lt,
     rtl_trns_lt,
     rtl_dlrs_12m_avg,
     rtl_dlrs_lt_avg,
     nextaction_id,
     lems,
     company_id,
     paid_12month_pc,
     paid_12month_cc,
     paid_12month_cash,
     filler
        )
    from 's3://idms-7933-internalfiles/neptune_apogee/Mgen_apogee_Extract.txt'
    iam_role '{iam}'
    acceptinvchars
    fixedwidth
    'last_transaction_overall_days:5,
    tot_parts:9,
    tot_dlrs_12m:10,
    tot_trns_12m:9,
    tot_dlrs_lt:10,
    tot_trns_lt:3,
    tot_dlrs_12m_avg:10,
    tot_dlrs_lt_avg:10,
    last_transaction_catalog_days:5,
    cat_part:9,
    cat_dlrs_12m:10,
    cat_trns_12m:9,
    cat_dlrs_lt:10,
    cat_trns_lt:3,
    cat_dlrs_12m_avg:10,
    cat_dlrs_lt_avg:10,
    last_transaction_web_days:5,
    web_parts:9,
    web_dlrs_12m:10,
    web_trns_12m:9,
    web_dlrs_lt:10,
    web_trns_lt:3,
    web_dlrs_12m_avg:10,
    web_dlrs_lt_avg:10,
    last_transaction_retail_days:5,
    rtl_parts:9,
    rtl_dlrs_12m:10,
    rtl_trns_12m:9,
    rtl_dlrs_lt:10,
    rtl_trns_lt:3,
    rtl_dlrs_12m_avg:10,
    rtl_dlrs_lt_avg:10,
    nextaction_id:60,
    lems:18,
    company_id:12,
    paid_12month_pc:1,
    paid_12month_cc:1,
    paid_12month_cash:1,
    filler:2';


