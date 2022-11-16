/*
NonProfit Mgen
Data Source: child2_apogee_new (mgen), cons univ  ON company_id
Condition:
OrderBy:
Other: pass 0 for nulls

 Update Schedule: Monthly
*/

unload ('select
        nvl(b.individual_id, ''''),
        nvl(a.company_id, ''''),
        nvl(b.lems, ''''),
        nvl(last_transaction_overall_days, 0),
        nvl(tot_parts, 0),
        nvl(tot_dlrs_12m, 0),
        nvl(tot_trns_12m, 0),
        nvl(tot_dlrs_lt, 0),
        nvl(tot_trns_lt, 0),
        nvl(tot_dlrs_12m_avg, 0),
        nvl(tot_dlrs_lt_avg, 0),
        nvl(last_transaction_catalog_days, 0),
        nvl(cat_part, 0),
        nvl(cat_dlrs_12m, 0),
        nvl(cat_trns_12m, 0),
        nvl(cat_dlrs_lt, 0),
        nvl(cat_trns_lt, 0),
        nvl(cat_dlrs_12m_avg, 0),
        nvl(cat_dlrs_lt_avg, 0),
        nvl(last_transaction_web_days, 0),
        nvl(web_parts, 0),
        nvl(web_dlrs_12m, 0),
        nvl(web_trns_12m, 0),
        nvl(web_dlrs_lt, 0),
        nvl(web_trns_lt, 0),
        nvl(web_dlrs_12m_avg, 0),
        nvl(web_dlrs_lt_avg, 0),
        nvl(last_transaction_retail_days, 0),
        nvl(rtl_parts, 0),
        nvl(rtl_dlrs_12m, 0),
        nvl(rtl_trns_12m, 0),
        nvl(rtl_dlrs_lt, 0),
        nvl(rtl_trns_lt, 0),
        nvl(rtl_dlrs_12m_avg, 0),
        nvl(rtl_dlrs_lt_avg, 0),
        nvl(trim(nextaction_id), ''''),
        nvl(a.lems, ''''),
        nvl(paid_12month_pc, ''''),
        nvl(paid_12month_cc, ''''),
        nvl(paid_12month_cash, '''')
--        ,hh_status_code  -- 20221010 CB
    from exclude_nonprofit_child2_74 a
        inner join ctas_cons_unv_individ_tobedropped b
        on a.company_id = b.company_id')
    to 's3://{s3-apogee}{export_mmdb_6}'
    iam_role '{iam}'
    encrypted
    cleanpath
    gzip
    delimiter as '|'
    maxfilesize 6 gb;


insert into apogee_export_count
    (tablename, record_count)
select 'nonprofit_mgen',  count(*)
from exclude_nonprofit_child2_74 a
    inner join ctas_cons_unv_individ_tobedropped b
    on a.company_id = b.company_id;

