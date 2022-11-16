/*
L2_Voter Data
Data Source: l2-ext39, cons univ  ON company_id
Condition:
OrderBy:
Other: pass 0 for nulls

Update Schedule: Monthly
*/


-- unload apogee link table for mgen update
-- to 's3://{s3-internal}/neptune_apogee/apogee_linked_file.txt'
unload ('select lems,
        company_id,
        donormatch,
        nvl(ltd_months_since_last_donation_date, 0),
        ltd_last_donation_date,
        nvl(ltd_number_of_list_sources, 0),
        nvl(ltd_average_donation_per_transaction, 0),
        nvl(m12_number_of_list_sources, 0),
        nvl(m12_average_donation_per_transaction, 0),
        nvl(m12_cat_ac_number_of_donation, 0),
        nvl(m12_cat_an_number_of_donation, 0),
        nvl(m12_cat_cd_number_of_donation, 0),
        nvl(m12_cat_cg_number_of_donation, 0),
        nvl(m12_cat_ch_number_of_donation, 0),
        nvl(m12_cat_co_number_of_donation, 0),
        nvl(m12_cat_cs_number_of_donation, 0),
        nvl(m12_cat_ea_number_of_donation, 0),
        nvl(m12_cat_et_number_of_donation, 0),
        nvl(m12_cat_hg_number_of_donation, 0),
        nvl(m12_cat_hs_number_of_donation, 0),
        nvl(m12_cat_li_number_of_donation, 0),
        nvl(m12_cat_nt_number_of_donation, 0),
        nvl(m12_cat_pa_number_of_donation, 0),
        nvl(m12_cat_rc_number_of_donation, 0),
        nvl(m12_cat_rd_number_of_donation, 0),
        nvl(m12_cat_ri_number_of_donation, 0),
        nvl(m12_cat_rj_number_of_donation, 0),
        nvl(m12_cat_ss_number_of_donation, 0),
        nvl(m12_cat_sw_number_of_donation, 0),
        nvl(m12_cat_vt_number_of_donation, 0),
        nvl(m48_number_of_donation_dollar_cash, 0),
        nvl(m48_number_of_donation_dollar_check, 0),
        nvl(m48_cat_an_number_of_donation, 0),
        nvl(m48_cat_ac_number_of_donation, 0),
        nvl(m48_cat_cd_number_of_donation, 0),
        nvl(m48_cat_cs_number_of_donation, 0),
        nvl(m48_cat_cg_number_of_donation, 0),
        nvl(m48_cat_ch_number_of_donation, 0),
        nvl(m48_cat_co_number_of_donation, 0),
        nvl(m48_cat_ea_number_of_donation, 0),
        nvl(m48_cat_et_number_of_donation, 0),
        nvl(m48_cat_hg_number_of_donation, 0),
        nvl(m48_cat_hs_number_of_donation, 0),
        nvl(m48_cat_rd_number_of_donation, 0),
        nvl(m48_cat_ri_number_of_donation, 0),
        nvl(m48_cat_li_number_of_donation, 0),
        nvl(m48_cat_nt_number_of_donation, 0),
        nvl(m48_cat_pa_number_of_donation, 0),
        nvl(m48_cat_rc_number_of_donation, 0),
        nvl(m48_cat_rj_number_of_donation, 0),
        nvl(m48_cat_ss_number_of_donation, 0),
        nvl(m48_cat_vt_number_of_donation, 0),
        nvl(m48_cat_sw_number_of_donation, 0),
        nvl(m48_number_of_list_sources, 0),
        nvl(m48_total_number_of_donations, 0),
        nvl(m48_total_dollar_donations, 0),
        nvl(ltd_number_of_donations_0_3_months_ago, 0),
        nvl(ltd_number_of_donations_0_6_months_ago, 0),
        nvl(ltd_number_of_donations_0_12_months_ago, 0),
        nvl(ltd_number_of_donations_13_24_months_ago, 0),
        nvl(ltd_number_of_donations_over_24_months_ago, 0),
        ltd_last_donate_yyyymm
    from exclude_nonprofit_tblexternal39_74')
    to 's3://{s3-apogee}{export_mgen_8}'
    iam_role '{iam}'
    encrypted
    cleanpath
    gzip
    delimiter as '|'
    parallel off
    maxfilesize 6 gb;


insert into apogee_export_count
    (tablename, record_count)
select 'apogee_link_mgen_update', count(*)
from exclude_nonprofit_tblexternal39_74;
