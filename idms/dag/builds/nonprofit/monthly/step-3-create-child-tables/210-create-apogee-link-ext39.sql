/*
 Create Apogee Link Table tblExternal39.
 Source is Main Table, Spark Rollup Tables
 20221028 cb: distinct on lems, no blank lems
 Update Schedule: Monthly
*/

-- get cons universe (maintable_name) columns
drop table if exists ctas_cons_univ_hhlems_tobedropped;
create table ctas_cons_univ_hhlems_tobedropped as
    (
        select distinct max(company_id) as company_id,
                        lems as lemsmatchcode
        from {maintable_name}
        where nvl(lems, ' ') != ' '
        group by lems
    );


-- create external table
drop table if exists exclude_nonprofit_tblexternal39_{dbid};
create table exclude_nonprofit_tblexternal39_{dbid}
(
    lems                                       varchar(18)   ,
    company_id                                 varchar(25)   encode zstd,
    donormatch                                 char(1)       encode zstd,
    ltd_months_since_last_donation_date        integer       encode az64,
    ltd_last_donation_date                     char(8)       encode zstd,
    ltd_number_of_list_sources                 integer       encode az64,
    ltd_average_donation_per_transaction       integer       encode az64,
    m12_number_of_list_sources                 integer       encode az64,
    m12_average_donation_per_transaction       integer       encode az64,
    m12_cat_ac_number_of_donation              integer       encode az64,
    m12_cat_an_number_of_donation              integer       encode az64,
    m12_cat_cd_number_of_donation              integer       encode az64,
    m12_cat_cg_number_of_donation              integer       encode az64,
    m12_cat_ch_number_of_donation              integer       encode az64,
    m12_cat_co_number_of_donation              integer       encode az64,
    m12_cat_cs_number_of_donation              integer       encode az64,
    m12_cat_ea_number_of_donation              integer       encode az64,
    m12_cat_et_number_of_donation              integer       encode az64,
    m12_cat_hg_number_of_donation              integer       encode az64,
    m12_cat_hs_number_of_donation              integer       encode az64,
    m12_cat_li_number_of_donation              integer       encode az64,
    m12_cat_nt_number_of_donation              integer       encode az64,
    m12_cat_pa_number_of_donation              integer       encode az64,
    m12_cat_rc_number_of_donation              integer       encode az64,
    m12_cat_rd_number_of_donation              integer       encode az64,
    m12_cat_ri_number_of_donation              integer       encode az64,
    m12_cat_rj_number_of_donation              integer       encode az64,
    m12_cat_ss_number_of_donation              integer       encode az64,
    m12_cat_sw_number_of_donation              integer       encode az64,
    m12_cat_vt_number_of_donation              integer       encode az64,
    m48_number_of_donation_dollar_cash         integer       encode az64,
    m48_number_of_donation_dollar_check        integer       encode az64,
    m48_cat_an_number_of_donation              integer       encode az64,
    m48_cat_ac_number_of_donation              integer       encode az64,
    m48_cat_cd_number_of_donation              integer       encode az64,
    m48_cat_cs_number_of_donation              integer       encode az64,
    m48_cat_cg_number_of_donation              integer       encode az64,
    m48_cat_ch_number_of_donation              integer       encode az64,
    m48_cat_co_number_of_donation              integer       encode az64,
    m48_cat_ea_number_of_donation              integer       encode az64,
    m48_cat_et_number_of_donation              integer       encode az64,
    m48_cat_hg_number_of_donation              integer       encode az64,
    m48_cat_hs_number_of_donation              integer       encode az64,
    m48_cat_rd_number_of_donation              integer       encode az64,
    m48_cat_ri_number_of_donation              integer       encode az64,
    m48_cat_li_number_of_donation              integer       encode az64,
    m48_cat_nt_number_of_donation              integer       encode az64,
    m48_cat_pa_number_of_donation              integer       encode az64,
    m48_cat_rc_number_of_donation              integer       encode az64,
    m48_cat_rj_number_of_donation              integer       encode az64,
    m48_cat_ss_number_of_donation              integer       encode az64,
    m48_cat_vt_number_of_donation              integer       encode az64,
    m48_cat_sw_number_of_donation              integer       encode az64,
    m48_number_of_list_sources                 integer       encode az64,
    m48_total_number_of_donations              integer       encode az64,
    m48_total_dollar_donations                 integer       encode az64,
    ltd_number_of_donations_0_3_months_ago     integer       encode az64,
    ltd_number_of_donations_0_6_months_ago     integer       encode az64,
    ltd_number_of_donations_0_12_months_ago    integer       encode az64,
    ltd_number_of_donations_13_24_months_ago   integer       encode az64,
    ltd_number_of_donations_over_24_months_ago integer       encode az64,
    ltd_last_donate_yyyymm                     char(6)       encode zstd
)
    diststyle key
    distkey (lems)
    sortkey (lems)
;

insert into exclude_nonprofit_tblexternal39_{dbid}
    (select lemsmatchcode as lems
          , ltd.company_id
          , 'Y' as donormatch
          , ltd_months_since_last_donation_date
          , ltd_last_donation_date
          , ltd_number_of_list_sources
          , ltd_average_donation_per_transaction
          , m12_number_of_list_sources
          , m12_average_donation_per_transaction
          , m12_cat_ac_number_of_donation
          , m12_cat_an_number_of_donation
          , m12_cat_cd_number_of_donation
          , m12_cat_cg_number_of_donation
          , m12_cat_ch_number_of_donation
          , m12_cat_co_number_of_donation
          , m12_cat_cs_number_of_donation
          , m12_cat_ea_number_of_donation
          , m12_cat_et_number_of_donation
          , m12_cat_hg_number_of_donation
          , m12_cat_hs_number_of_donation
          , m12_cat_li_number_of_donation
          , m12_cat_nt_number_of_donation
          , m12_cat_pa_number_of_donation
          , m12_cat_rc_number_of_donation
          , m12_cat_rd_number_of_donation
          , m12_cat_ri_number_of_donation
          , m12_cat_rj_number_of_donation
          , m12_cat_ss_number_of_donation
          , m12_cat_sw_number_of_donation
          , m12_cat_vt_number_of_donation
          , m48_number_of_donation_dollar_cash
          , m48_number_of_donation_dollar_check
          , m48_cat_an_number_of_donation
          , m48_cat_ac_number_of_donation
          , m48_cat_cd_number_of_donation
          , m48_cat_cs_number_of_donation
          , m48_cat_cg_number_of_donation
          , m48_cat_ch_number_of_donation
          , m48_cat_co_number_of_donation
          , m48_cat_ea_number_of_donation
          , m48_cat_et_number_of_donation
          , m48_cat_hg_number_of_donation
          , m48_cat_hs_number_of_donation
          , m48_cat_rd_number_of_donation
          , m48_cat_ri_number_of_donation
          , m48_cat_li_number_of_donation
          , m48_cat_nt_number_of_donation
          , m48_cat_pa_number_of_donation
          , m48_cat_rc_number_of_donation
          , m48_cat_rj_number_of_donation
          , m48_cat_ss_number_of_donation
          , m48_cat_vt_number_of_donation
          , m48_cat_sw_number_of_donation
          , m48_number_of_list_sources
          , m48_total_number_of_donations
          , m48_total_dollar_donations
          , ltd_number_of_donations_0_3_months_ago
          , ltd_number_of_donations_0_6_months_ago
          , ltd_number_of_donations_0_12_months_ago
          , ltd_number_of_donations_13_24_months_ago
          , ltd_number_of_donations_over_24_months_ago
          , left(ltd_last_donation_yyyymm, 6) as ltd_last_donate_yyyymm
     from exclude_sum_ltd_np ltd
           left outer join exclude_sum_cat_np cat on ltd.company_id = cat.company_id
           inner join ctas_cons_univ_hhlems_tobedropped hhlems on ltd.company_id = hhlems.company_id
);

drop table if exists ctas_cons_univ_hhlems_tobedropped;
