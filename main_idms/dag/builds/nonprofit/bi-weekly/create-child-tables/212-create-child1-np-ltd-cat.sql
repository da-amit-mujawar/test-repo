/*
 Create Apogee Child1 Table HouseHold Summary.
 Source is Spark Rollup Tables

 Update Schedule: Bi-Weekly, runs on 15th and 28th of every month
 8/11/2022 : Jayesh removed depedency on ctas_ext39_tobedropped of step 210 by left joining tblMain
*/


-- create child1
drop table if exists exclude_nonprofit_child1_{dbid};
create table exclude_nonprofit_child1_{dbid}
(
    company_id                                    varchar(25)   ,
    lemsmatchcode                                 varchar(18)   encode zstd,
   -- externaltableflag                             char(1)       encode zstd,
   -- donormatch                                    char(1)       encode zstd,
    ltd_average_donation_per_transaction          integer       encode az64,
    ltd_avg_donation_range                        char(1)       encode zstd,
    ltd_first_donation_date                       varchar(8)    encode zstd,
    ltd_highest_donation_amount_ever              integer       encode az64,
    ltd_last_donation_date                        varchar(8)    encode zstd,
    ltd_last_donation_yyyymm                      varchar(6)    encode zstd,
    ltd_last_payment_method                       char(1)       encode zstd,
    ltd_last_response_channel                     char(1)       encode zstd,
    ltd_lowest_donation_amount_ever               integer       encode az64,
    ltd_months_since_first_donation_date          integer       encode az64,
    ltd_months_since_last_donation_date           integer       encode az64,
    ltd_months_since_last_donation_mail           integer       encode az64,
    ltd_months_since_last_donation_mobile         integer       encode az64,
    ltd_months_since_last_donation_phone          integer       encode az64,
    ltd_months_since_last_donation_web            integer       encode az64,
    ltd_number_of_donations_0_12_months_ago       integer       encode az64,
    ltd_number_of_donations_0_24_months_ago       integer       encode az64,
    ltd_number_of_donations_0_3_months_ago        integer       encode az64,
    ltd_number_of_donations_0_6_months_ago        integer       encode az64,
    ltd_number_of_donations_13_24_months_ago      integer       encode az64,
    ltd_number_of_donations_2_3_months_ago        integer       encode az64,
    ltd_number_of_donations_4_6_months_ago        integer       encode az64,
    ltd_number_of_donations_7_12_months_ago       integer       encode az64,
    ltd_number_of_donations_over_24_months_ago    integer       encode az64,
    ltd_number_of_donations_past_30_days          integer       encode az64,
    ltd_number_of_list_sources                    integer       encode az64,
    ltd_total_dollar_donations                    integer       encode az64,
    ltd_total_number_of_donations                 integer       encode az64,
    m12_average_donation_per_transaction          integer       encode az64,
    m12_highest_donation_amount                   integer       encode az64,
    m12_lowest_donation_amount                    integer       encode az64,
    m12_number_of_list_sources                    integer       encode az64,
    m12_total_dollar_donations                    integer       encode az64,
    m12_total_number_of_donations                 integer       encode az64,
    m24_average_donation_per_transaction          integer       encode az64,
    m24_highest_donation_amount                   integer       encode az64,
    m24_lowest_donation_amount                    integer       encode az64,
    m24_number_of_list_sources                    integer       encode az64,
    m24_total_dollar_donations                    integer       encode az64,
    m24_total_number_of_donations                 integer       encode az64,
    m48_average_days_between_donations            integer       encode az64,
    m48_average_donation_per_transaction          integer       encode az64,
    m48_highest_donation_amount                   integer       encode az64,
    m48_lowest_donation_amount                    integer       encode az64,
    m48_number_of_donation_dollar_1_4_amount      integer       encode az64,
    m48_number_of_donation_dollar_10_14_amount    integer       encode az64,
    m48_number_of_donation_dollar_100_249_amount  integer       encode az64,
    m48_number_of_donation_dollar_15_24_amount    integer       encode az64,
    m48_number_of_donation_dollar_25_49_amount    integer       encode az64,
    m48_number_of_donation_dollar_250_499_amount  integer       encode az64,
    m48_number_of_donation_dollar_5_9_amount      integer       encode az64,
    m48_number_of_donation_dollar_50_99_amount    integer       encode az64,
    m48_number_of_donation_dollar_500_more_amount integer       encode az64,
    m48_number_of_list_sources                    integer       encode az64,
    m48_total_dollar_donations                    integer       encode az64,
    m48_total_number_of_donations                 integer       encode az64,
	m48_avg_dollar_donation_via_mail              integer       encode az64,
	m48_avg_dollar_donation_via_mobile            integer       encode az64,
	m48_avg_dollar_donation_via_phone             integer       encode az64,
	m48_avg_dollar_donation_via_web               integer       encode az64,
	m48_dollar_donation_via_mail                  integer       encode az64,
	m48_dollar_donation_via_mobile                integer       encode az64,
	m48_dollar_donation_via_phone                 integer       encode az64,
	m48_dollar_donation_via_web                   integer       encode az64,
	m48_number_of_donation_dollar_amex            integer       encode az64,
	m48_number_of_donation_dollar_cash            integer       encode az64,
	m48_number_of_donation_dollar_check           integer       encode az64,
	m48_number_of_donation_dollar_master_card     integer       encode az64,
	m48_number_of_donation_dollar_other           integer       encode az64,
	m48_number_of_donation_dollar_visa            integer       encode az64,
	m48_number_of_donation_via_mail               integer       encode az64,
	m48_number_of_donation_via_mobile             integer       encode az64,
	m48_number_of_donation_via_phone              integer       encode az64,
	m48_number_of_donation_via_web                integer       encode az64,
    number_of_donation_april                      integer       encode az64,
    number_of_donation_august                     integer       encode az64,
    number_of_donation_december                   integer       encode az64,
    number_of_donation_february                   integer       encode az64,
    number_of_donation_january                    integer       encode az64,
    number_of_donation_july                       integer       encode az64,
    number_of_donation_june                       integer       encode az64,
    number_of_donation_march                      integer       encode az64,
    number_of_donation_may                        integer       encode az64,
    number_of_donation_november                   integer       encode az64,
    number_of_donation_october                    integer       encode az64,
    number_of_donation_september                  integer       encode az64,
    pricat_ac                                     integer       encode az64,
    pricat_an                                     integer       encode az64,
    pricat_cd                                     integer       encode az64,
    pricat_cg                                     integer       encode az64,
    pricat_ch                                     integer       encode az64,
    pricat_co                                     integer       encode az64,
    pricat_cs                                     integer       encode az64,
    pricat_ea                                     integer       encode az64,
    pricat_et                                     integer       encode az64,
    pricat_hg                                     integer       encode az64,
    pricat_hs                                     integer       encode az64,
    pricat_li                                     integer       encode az64,
    pricat_nt                                     integer       encode az64,
    pricat_pa                                     integer       encode az64,
    pricat_rc                                     integer       encode az64,
    pricat_rd                                     integer       encode az64,
    pricat_ri                                     integer       encode az64,
    pricat_rj                                     integer       encode az64,
    pricat_ss                                     integer       encode az64,
    pricat_sw                                     integer       encode az64,
    pricat_vt                                     integer       encode az64,
    m12_cat_ac_avg_dollar_donation                integer       encode az64,
    m12_cat_ac_dollar_donation                    integer       encode az64,
    m12_cat_ac_months_since_last_donation         integer       encode az64,
    m12_cat_ac_number_of_donation                 integer       encode az64,
    m12_cat_an_avg_dollar_donation                integer       encode az64,
    m12_cat_an_dollar_donation                    integer       encode az64,
    m12_cat_an_months_since_last_donation         integer       encode az64,
    m12_cat_an_number_of_donation                 integer       encode az64,
    m12_cat_cd_avg_dollar_donation                integer       encode az64,
    m12_cat_cd_dollar_donation                    integer       encode az64,
    m12_cat_cd_months_since_last_donation         integer       encode az64,
    m12_cat_cd_number_of_donation                 integer       encode az64,
    m12_cat_cg_avg_dollar_donation                integer       encode az64,
    m12_cat_cg_dollar_donation                    integer       encode az64,
    m12_cat_cg_months_since_last_donation         integer       encode az64,
    m12_cat_cg_number_of_donation                 integer       encode az64,
    m12_cat_ch_avg_dollar_donation                integer       encode az64,
    m12_cat_ch_dollar_donation                    integer       encode az64,
    m12_cat_ch_months_since_last_donation         integer       encode az64,
    m12_cat_ch_number_of_donation                 integer       encode az64,
    m12_cat_co_avg_dollar_donation                integer       encode az64,
    m12_cat_co_dollar_donation                    integer       encode az64,
    m12_cat_co_months_since_last_donation         integer       encode az64,
    m12_cat_co_number_of_donation                 integer       encode az64,
    m12_cat_cs_avg_dollar_donation                integer       encode az64,
    m12_cat_cs_dollar_donation                    integer       encode az64,
    m12_cat_cs_months_since_last_donation         integer       encode az64,
    m12_cat_cs_number_of_donation                 integer       encode az64,
    m12_cat_ea_avg_dollar_donation                integer       encode az64,
    m12_cat_ea_dollar_donation                    integer       encode az64,
    m12_cat_ea_months_since_last_donation         integer       encode az64,
    m12_cat_ea_number_of_donation                 integer       encode az64,
    m12_cat_ed_avg_dollar_donation                integer       encode az64,
    m12_cat_ed_dollar_donation                    integer       encode az64,
    m12_cat_ed_months_since_last_donation         integer       encode az64,
    m12_cat_ed_number_of_donation                 integer       encode az64,
    m12_cat_er_avg_dollar_donation                integer       encode az64,
    m12_cat_er_dollar_donation                    integer       encode az64,
    m12_cat_er_months_since_last_donation         integer       encode az64,
    m12_cat_er_number_of_donation                 integer       encode az64,
    m12_cat_et_avg_dollar_donation                integer       encode az64,
    m12_cat_et_dollar_donation                    integer       encode az64,
    m12_cat_et_months_since_last_donation         integer       encode az64,
    m12_cat_et_number_of_donation                 integer       encode az64,
    m12_cat_hg_avg_dollar_donation                integer       encode az64,
    m12_cat_hg_dollar_donation                    integer       encode az64,
    m12_cat_hg_months_since_last_donation         integer       encode az64,
    m12_cat_hg_number_of_donation                 integer       encode az64,
    m12_cat_hs_avg_dollar_donation                integer       encode az64,
    m12_cat_hs_dollar_donation                    integer       encode az64,
    m12_cat_hs_months_since_last_donation         integer       encode az64,
    m12_cat_hs_number_of_donation                 integer       encode az64,
    m12_cat_li_avg_dollar_donation                integer       encode az64,
    m12_cat_li_dollar_donation                    integer       encode az64,
    m12_cat_li_months_since_last_donation         integer       encode az64,
    m12_cat_li_number_of_donation                 integer       encode az64,
    m12_cat_nt_avg_dollar_donation                integer       encode az64,
    m12_cat_nt_dollar_donation                    integer       encode az64,
    m12_cat_nt_months_since_last_donation         integer       encode az64,
    m12_cat_nt_number_of_donation                 integer       encode az64,
    m12_cat_pa_avg_dollar_donation                integer       encode az64,
    m12_cat_pa_dollar_donation                    integer       encode az64,
    m12_cat_pa_months_since_last_donation         integer       encode az64,
    m12_cat_pa_number_of_donation                 integer       encode az64,
    m12_cat_rc_avg_dollar_donation                integer       encode az64,
    m12_cat_rc_dollar_donation                    integer       encode az64,
    m12_cat_rc_months_since_last_donation         integer       encode az64,
    m12_cat_rc_number_of_donation                 integer       encode az64,
    m12_cat_rd_avg_dollar_donation                integer       encode az64,
    m12_cat_rd_dollar_donation                    integer       encode az64,
    m12_cat_rd_months_since_last_donation         integer       encode az64,
    m12_cat_rd_number_of_donation                 integer       encode az64,
    m12_cat_ri_avg_dollar_donation                integer       encode az64,
    m12_cat_ri_dollar_donation                    integer       encode az64,
    m12_cat_ri_months_since_last_donation         integer       encode az64,
    m12_cat_ri_number_of_donation                 integer       encode az64,
    m12_cat_rj_avg_dollar_donation                integer       encode az64,
    m12_cat_rj_dollar_donation                    integer       encode az64,
    m12_cat_rj_months_since_last_donation         integer       encode az64,
    m12_cat_rj_number_of_donation                 integer       encode az64,
    m12_cat_sc_avg_dollar_donation                integer       encode az64,
    m12_cat_sc_dollar_donation                    integer       encode az64,
    m12_cat_sc_months_since_last_donation         integer       encode az64,
    m12_cat_sc_number_of_donation                 integer       encode az64,
    m12_cat_ss_avg_dollar_donation                integer       encode az64,
    m12_cat_ss_dollar_donation                    integer       encode az64,
    m12_cat_ss_months_since_last_donation         integer       encode az64,
    m12_cat_ss_number_of_donation                 integer       encode az64,
    m12_cat_sw_avg_dollar_donation                integer       encode az64,
    m12_cat_sw_dollar_donation                    integer       encode az64,
    m12_cat_sw_months_since_last_donation         integer       encode az64,
    m12_cat_sw_number_of_donation                 integer       encode az64,
    m12_cat_vt_avg_dollar_donation                integer       encode az64,
    m12_cat_vt_dollar_donation                    integer       encode az64,
    m12_cat_vt_months_since_last_donation         integer       encode az64,
    m12_cat_vt_number_of_donation                 integer       encode az64,
    m48_cat_ac_avg_dollar_donation                integer       encode az64,
    m48_cat_ac_dollar_donation                    integer       encode az64,
    m48_cat_ac_months_since_last_donation         integer       encode az64,
    m48_cat_ac_number_of_donation                 integer       encode az64,
    m48_cat_an_avg_dollar_donation                integer       encode az64,
    m48_cat_an_dollar_donation                    integer       encode az64,
    m48_cat_an_months_since_last_donation         integer       encode az64,
    m48_cat_an_number_of_donation                 integer       encode az64,
    m48_cat_cd_avg_dollar_donation                integer       encode az64,
    m48_cat_cd_dollar_donation                    integer       encode az64,
    m48_cat_cd_months_since_last_donation         integer       encode az64,
    m48_cat_cd_number_of_donation                 integer       encode az64,
    m48_cat_cg_avg_dollar_donation                integer       encode az64,
    m48_cat_cg_dollar_donation                    integer       encode az64,
    m48_cat_cg_months_since_last_donation         integer       encode az64,
    m48_cat_cg_number_of_donation                 integer       encode az64,
    m48_cat_ch_avg_dollar_donation                integer       encode az64,
    m48_cat_ch_dollar_donation                    integer       encode az64,
    m48_cat_ch_months_since_last_donation         integer       encode az64,
    m48_cat_ch_number_of_donation                 integer       encode az64,
    m48_cat_co_avg_dollar_donation                integer       encode az64,
    m48_cat_co_dollar_donation                    integer       encode az64,
    m48_cat_co_months_since_last_donation         integer       encode az64,
    m48_cat_co_number_of_donation                 integer       encode az64,
    m48_cat_cs_avg_dollar_donation                integer       encode az64,
    m48_cat_cs_dollar_donation                    integer       encode az64,
    m48_cat_cs_months_since_last_donation         integer       encode az64,
    m48_cat_cs_number_of_donation                 integer       encode az64,
    m48_cat_ea_avg_dollar_donation                integer       encode az64,
    m48_cat_ea_dollar_donation                    integer       encode az64,
    m48_cat_ea_months_since_last_donation         integer       encode az64,
    m48_cat_ea_number_of_donation                 integer       encode az64,
    m48_cat_ed_avg_dollar_donation                integer       encode az64,
    m48_cat_ed_dollar_donation                    integer       encode az64,
    m48_cat_ed_months_since_last_donation         integer       encode az64,
    m48_cat_ed_number_of_donation                 integer       encode az64,
    m48_cat_er_avg_dollar_donation                integer       encode az64,
    m48_cat_er_dollar_donation                    integer       encode az64,
    m48_cat_er_months_since_last_donation         integer       encode az64,
    m48_cat_er_number_of_donation                 integer       encode az64,
    m48_cat_et_avg_dollar_donation                integer       encode az64,
    m48_cat_et_dollar_donation                    integer       encode az64,
    m48_cat_et_months_since_last_donation         integer       encode az64,
    m48_cat_et_number_of_donation                 integer       encode az64,
    m48_cat_hg_avg_dollar_donation                integer       encode az64,
    m48_cat_hg_dollar_donation                    integer       encode az64,
    m48_cat_hg_months_since_last_donation         integer       encode az64,
    m48_cat_hg_number_of_donation                 integer       encode az64,
    m48_cat_hs_avg_dollar_donation                integer       encode az64,
    m48_cat_hs_dollar_donation                    integer       encode az64,
    m48_cat_hs_months_since_last_donation         integer       encode az64,
    m48_cat_hs_number_of_donation                 integer       encode az64,
    m48_cat_li_avg_dollar_donation                integer       encode az64,
    m48_cat_li_dollar_donation                    integer       encode az64,
    m48_cat_li_months_since_last_donation         integer       encode az64,
    m48_cat_li_number_of_donation                 integer       encode az64,
    m48_cat_nt_avg_dollar_donation                integer       encode az64,
    m48_cat_nt_dollar_donation                    integer       encode az64,
    m48_cat_nt_months_since_last_donation         integer       encode az64,
    m48_cat_nt_number_of_donation                 integer       encode az64,
    m48_cat_pa_avg_dollar_donation                integer       encode az64,
    m48_cat_pa_dollar_donation                    integer       encode az64,
    m48_cat_pa_months_since_last_donation         integer       encode az64,
    m48_cat_pa_number_of_donation                 integer       encode az64,
    m48_cat_rc_avg_dollar_donation                integer       encode az64,
    m48_cat_rc_dollar_donation                    integer       encode az64,
    m48_cat_rc_months_since_last_donation         integer       encode az64,
    m48_cat_rc_number_of_donation                 integer       encode az64,
    m48_cat_rd_avg_dollar_donation                integer       encode az64,
    m48_cat_rd_dollar_donation                    integer       encode az64,
    m48_cat_rd_months_since_last_donation         integer       encode az64,
    m48_cat_rd_number_of_donation                 integer       encode az64,
    m48_cat_ri_avg_dollar_donation                integer       encode az64,
    m48_cat_ri_dollar_donation                    integer       encode az64,
    m48_cat_ri_months_since_last_donation         integer       encode az64,
    m48_cat_ri_number_of_donation                 integer       encode az64,
    m48_cat_rj_avg_dollar_donation                integer       encode az64,
    m48_cat_rj_dollar_donation                    integer       encode az64,
    m48_cat_rj_months_since_last_donation         integer       encode az64,
    m48_cat_rj_number_of_donation                 integer       encode az64,
    m48_cat_sc_avg_dollar_donation                integer       encode az64,
    m48_cat_sc_dollar_donation                    integer       encode az64,
    m48_cat_sc_months_since_last_donation         integer       encode az64,
    m48_cat_sc_number_of_donation                 integer       encode az64,
    m48_cat_ss_avg_dollar_donation                integer       encode az64,
    m48_cat_ss_dollar_donation                    integer       encode az64,
    m48_cat_ss_months_since_last_donation         integer       encode az64,
    m48_cat_ss_number_of_donation                 integer       encode az64,
    m48_cat_sw_avg_dollar_donation                integer       encode az64,
    m48_cat_sw_dollar_donation                    integer       encode az64,
    m48_cat_sw_months_since_last_donation         integer       encode az64,
    m48_cat_sw_number_of_donation                 integer       encode az64,
    m48_cat_vt_avg_dollar_donation                integer       encode az64,
	m48_cat_vt_dollar_donation                    integer       encode az64,
	m48_cat_vt_months_since_last_donation         integer       encode az64,
	m48_cat_vt_number_of_donation                 integer       encode az64,
	m12_cat_tot_ac_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_ac_dollar_donation                integer       encode az64,
	m12_cat_tot_ac_months_since_last_donation     integer       encode az64,
	m12_cat_tot_ac_number_of_donation             integer       encode az64,
	m12_cat_tot_an_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_an_dollar_donation                integer       encode az64,
	m12_cat_tot_an_months_since_last_donation     integer       encode az64,
	m12_cat_tot_an_number_of_donation             integer       encode az64,
	m12_cat_tot_cd_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_cd_dollar_donation                integer       encode az64,
	m12_cat_tot_cd_months_since_last_donation     integer       encode az64,
	m12_cat_tot_cd_number_of_donation             integer       encode az64,
	m12_cat_tot_cg_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_cg_dollar_donation                integer       encode az64,
	m12_cat_tot_cg_months_since_last_donation     integer       encode az64,
	m12_cat_tot_cg_number_of_donation             integer       encode az64,
	m12_cat_tot_ch_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_ch_dollar_donation                integer       encode az64,
	m12_cat_tot_ch_months_since_last_donation     integer       encode az64,
	m12_cat_tot_ch_number_of_donation             integer       encode az64,
	m12_cat_tot_co_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_co_dollar_donation                integer       encode az64,
	m12_cat_tot_co_months_since_last_donation     integer       encode az64,
	m12_cat_tot_co_number_of_donation             integer       encode az64,
	m12_cat_tot_cs_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_cs_dollar_donation                integer       encode az64,
	m12_cat_tot_cs_months_since_last_donation     integer       encode az64,
	m12_cat_tot_cs_number_of_donation             integer       encode az64,
	m12_cat_tot_ea_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_ea_dollar_donation                integer       encode az64,
	m12_cat_tot_ea_months_since_last_donation     integer       encode az64,
	m12_cat_tot_ea_number_of_donation             integer       encode az64,
	m12_cat_tot_ed_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_ed_dollar_donation                integer       encode az64,
	m12_cat_tot_ed_months_since_last_donation     integer       encode az64,
	m12_cat_tot_ed_number_of_donation             integer       encode az64,
	m12_cat_tot_er_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_er_dollar_donation                integer       encode az64,
	m12_cat_tot_er_months_since_last_donation     integer       encode az64,
	m12_cat_tot_er_number_of_donation             integer       encode az64,
	m12_cat_tot_et_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_et_dollar_donation                integer       encode az64,
	m12_cat_tot_et_months_since_last_donation     integer       encode az64,
	m12_cat_tot_et_number_of_donation             integer       encode az64,
	m12_cat_tot_hg_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_hg_dollar_donation                integer       encode az64,
	m12_cat_tot_hg_months_since_last_donation     integer       encode az64,
	m12_cat_tot_hg_number_of_donation             integer       encode az64,
	m12_cat_tot_hs_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_hs_dollar_donation                integer       encode az64,
	m12_cat_tot_hs_months_since_last_donation     integer       encode az64,
	m12_cat_tot_hs_number_of_donation             integer       encode az64,
	m12_cat_tot_li_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_li_dollar_donation                integer       encode az64,
	m12_cat_tot_li_months_since_last_donation     integer       encode az64,
	m12_cat_tot_li_number_of_donation             integer       encode az64,
	m12_cat_tot_nt_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_nt_dollar_donation                integer       encode az64,
	m12_cat_tot_nt_months_since_last_donation     integer       encode az64,
	m12_cat_tot_nt_number_of_donation             integer       encode az64,
	m12_cat_tot_pa_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_pa_dollar_donation                integer       encode az64,
	m12_cat_tot_pa_months_since_last_donation     integer       encode az64,
	m12_cat_tot_pa_number_of_donation             integer       encode az64,
	m12_cat_tot_rc_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_rc_dollar_donation                integer       encode az64,
	m12_cat_tot_rc_months_since_last_donation     integer       encode az64,
	m12_cat_tot_rc_number_of_donation             integer       encode az64,
	m12_cat_tot_rd_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_rd_dollar_donation                integer       encode az64,
	m12_cat_tot_rd_months_since_last_donation     integer       encode az64,
	m12_cat_tot_rd_number_of_donation             integer       encode az64,
	m12_cat_tot_ri_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_ri_dollar_donation                integer       encode az64,
	m12_cat_tot_ri_months_since_last_donation     integer       encode az64,
	m12_cat_tot_ri_number_of_donation             integer       encode az64,
	m12_cat_tot_rj_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_rj_dollar_donation                integer       encode az64,
	m12_cat_tot_rj_months_since_last_donation     integer       encode az64,
	m12_cat_tot_rj_number_of_donation             integer       encode az64,
	m12_cat_tot_sc_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_sc_dollar_donation                integer       encode az64,
	m12_cat_tot_sc_months_since_last_donation     integer       encode az64,
	m12_cat_tot_sc_number_of_donation             integer       encode az64,
	m12_cat_tot_ss_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_ss_dollar_donation                integer       encode az64,
	m12_cat_tot_ss_months_since_last_donation     integer       encode az64,
	m12_cat_tot_ss_number_of_donation             integer       encode az64,
	m12_cat_tot_sw_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_sw_dollar_donation                integer       encode az64,
	m12_cat_tot_sw_months_since_last_donation     integer       encode az64,
	m12_cat_tot_sw_number_of_donation             integer       encode az64,
	m12_cat_tot_vt_avg_dollar_donation            integer       encode az64,
	m12_cat_tot_vt_dollar_donation                integer       encode az64,
	m12_cat_tot_vt_months_since_last_donation     integer       encode az64,
	m12_cat_tot_vt_number_of_donation             integer       encode az64,
	m48_cat_tot_ac_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_ac_dollar_donation                integer       encode az64,
	m48_cat_tot_ac_months_since_last_donation     integer       encode az64,
	m48_cat_tot_ac_number_of_donation             integer       encode az64,
	m48_cat_tot_an_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_an_dollar_donation                integer       encode az64,
	m48_cat_tot_an_months_since_last_donation     integer       encode az64,
	m48_cat_tot_an_number_of_donation             integer       encode az64,
	m48_cat_tot_cd_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_cd_dollar_donation                integer       encode az64,
	m48_cat_tot_cd_months_since_last_donation     integer       encode az64,
	m48_cat_tot_cd_number_of_donation             integer       encode az64,
	m48_cat_tot_cg_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_cg_dollar_donation                integer       encode az64,
	m48_cat_tot_cg_months_since_last_donation     integer       encode az64,
	m48_cat_tot_cg_number_of_donation             integer       encode az64,
	m48_cat_tot_ch_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_ch_dollar_donation                integer       encode az64,
	m48_cat_tot_ch_months_since_last_donation     integer       encode az64,
	m48_cat_tot_ch_number_of_donation             integer       encode az64,
	m48_cat_tot_co_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_co_dollar_donation                integer       encode az64,
	m48_cat_tot_co_months_since_last_donation     integer       encode az64,
	m48_cat_tot_co_number_of_donation             integer       encode az64,
	m48_cat_tot_cs_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_cs_dollar_donation                integer       encode az64,
	m48_cat_tot_cs_months_since_last_donation     integer       encode az64,
	m48_cat_tot_cs_number_of_donation             integer       encode az64,
	m48_cat_tot_ea_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_ea_dollar_donation                integer       encode az64,
	m48_cat_tot_ea_months_since_last_donation     integer       encode az64,
	m48_cat_tot_ea_number_of_donation             integer       encode az64,
	m48_cat_tot_ed_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_ed_dollar_donation                integer       encode az64,
	m48_cat_tot_ed_months_since_last_donation     integer       encode az64,
	m48_cat_tot_ed_number_of_donation             integer       encode az64,
	m48_cat_tot_er_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_er_dollar_donation                integer       encode az64,
	m48_cat_tot_er_months_since_last_donation     integer       encode az64,
	m48_cat_tot_er_number_of_donation             integer       encode az64,
	m48_cat_tot_et_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_et_dollar_donation                integer       encode az64,
	m48_cat_tot_et_months_since_last_donation     integer       encode az64,
	m48_cat_tot_et_number_of_donation             integer       encode az64,
	m48_cat_tot_hg_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_hg_dollar_donation                integer       encode az64,
	m48_cat_tot_hg_months_since_last_donation     integer       encode az64,
	m48_cat_tot_hg_number_of_donation             integer       encode az64,
	m48_cat_tot_hs_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_hs_dollar_donation                integer       encode az64,
	m48_cat_tot_hs_months_since_last_donation     integer       encode az64,
	m48_cat_tot_hs_number_of_donation             integer       encode az64,
	m48_cat_tot_li_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_li_dollar_donation                integer       encode az64,
	m48_cat_tot_li_months_since_last_donation     integer       encode az64,
	m48_cat_tot_li_number_of_donation             integer       encode az64,
	m48_cat_tot_nt_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_nt_dollar_donation                integer       encode az64,
	m48_cat_tot_nt_months_since_last_donation     integer       encode az64,
	m48_cat_tot_nt_number_of_donation             integer       encode az64,
	m48_cat_tot_pa_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_pa_dollar_donation                integer       encode az64,
	m48_cat_tot_pa_months_since_last_donation     integer       encode az64,
	m48_cat_tot_pa_number_of_donation             integer       encode az64,
	m48_cat_tot_rc_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_rc_dollar_donation                integer       encode az64,
	m48_cat_tot_rc_months_since_last_donation     integer       encode az64,
	m48_cat_tot_rc_number_of_donation             integer       encode az64,
	m48_cat_tot_rd_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_rd_dollar_donation                integer       encode az64,
	m48_cat_tot_rd_months_since_last_donation     integer       encode az64,
	m48_cat_tot_rd_number_of_donation             integer       encode az64,
	m48_cat_tot_ri_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_ri_dollar_donation                integer       encode az64,
	m48_cat_tot_ri_months_since_last_donation     integer       encode az64,
	m48_cat_tot_ri_number_of_donation             integer       encode az64,
	m48_cat_tot_rj_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_rj_dollar_donation                integer       encode az64,
	m48_cat_tot_rj_months_since_last_donation     integer       encode az64,
	m48_cat_tot_rj_number_of_donation             integer       encode az64,
	m48_cat_tot_sc_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_sc_dollar_donation                integer       encode az64,
	m48_cat_tot_sc_months_since_last_donation     integer       encode az64,
	m48_cat_tot_sc_number_of_donation             integer       encode az64,
	m48_cat_tot_ss_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_ss_dollar_donation                integer       encode az64,
	m48_cat_tot_ss_months_since_last_donation     integer       encode az64,
	m48_cat_tot_ss_number_of_donation             integer       encode az64,
	m48_cat_tot_sw_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_sw_dollar_donation                integer       encode az64,
	m48_cat_tot_sw_months_since_last_donation     integer       encode az64,
	m48_cat_tot_sw_number_of_donation             integer       encode az64,
	m48_cat_tot_vt_avg_dollar_donation            integer       encode az64,
	m48_cat_tot_vt_dollar_donation                integer       encode az64,
	m48_cat_tot_vt_months_since_last_donation     integer       encode az64,
	m48_cat_tot_vt_number_of_donation             integer       encode az64

)
    diststyle key
    distkey (company_id)
    sortkey (company_id)
;

insert into exclude_nonprofit_child1_{dbid}
    (select ltd2.company_id
          , ext392.lemsmatchcode
         -- , case when ext392.lemsmatchcode = '' then 'N' else 'Y' end as externaltableflag
         -- , case when ext392.lemsmatchcode = '' then 'N' else 'Y' end as donormatch
          , ltd2.ltd_average_donation_per_transaction
          , ltd2.ltd_avg_donation_range
          , ltd2.ltd_first_donation_date
          , ltd2.ltd_highest_donation_amount_ever
          , ltd2.ltd_last_donation_date
          , ltd2.ltd_last_donation_yyyymm
          , ltd2.ltd_last_payment_method
          , ltd2.ltd_last_response_channel
          , ltd2.ltd_lowest_donation_amount_ever
          , ltd2.ltd_months_since_first_donation_date
          , ltd2.ltd_months_since_last_donation_date
          , ltd2.ltd_months_since_last_donation_mail
          , ltd2.ltd_months_since_last_donation_mobile
          , ltd2.ltd_months_since_last_donation_phone
          , ltd2.ltd_months_since_last_donation_web
          , ltd2.ltd_number_of_donations_0_12_months_ago
          , ltd2.ltd_number_of_donations_0_24_months_ago
          , ltd2.ltd_number_of_donations_0_3_months_ago
          , ltd2.ltd_number_of_donations_0_6_months_ago
          , ltd2.ltd_number_of_donations_13_24_months_ago
          , ltd2.ltd_number_of_donations_2_3_months_ago
          , ltd2.ltd_number_of_donations_4_6_months_ago
          , ltd2.ltd_number_of_donations_7_12_months_ago
          , ltd2.ltd_number_of_donations_over_24_months_ago
          , ltd2.ltd_number_of_donations_past_30_days
          , ltd2.ltd_number_of_list_sources
          , ltd2.ltd_total_dollar_donations
          , ltd2.ltd_total_number_of_donations
          , ltd2.m12_average_donation_per_transaction
          , ltd2.m12_highest_donation_amount
          , ltd2.m12_lowest_donation_amount
          , ltd2.m12_number_of_list_sources
          , ltd2.m12_total_dollar_donations
          , ltd2.m12_total_number_of_donations
          , ltd2.m24_average_donation_per_transaction
          , ltd2.m24_highest_donation_amount
          , ltd2.m24_lowest_donation_amount
          , ltd2.m24_number_of_list_sources
          , ltd2.m24_total_dollar_donations
          , ltd2.m24_total_number_of_donations
          , ltd2.m48_average_days_between_donations
          , ltd2.m48_average_donation_per_transaction
          , ltd2.m48_highest_donation_amount
          , ltd2.m48_lowest_donation_amount
          , ltd2.m48_number_of_donation_dollar_1_4_amount
          , ltd2.m48_number_of_donation_dollar_10_14_amount
          , ltd2.m48_number_of_donation_dollar_100_249_amount
          , ltd2.m48_number_of_donation_dollar_15_24_amount
          , ltd2.m48_number_of_donation_dollar_25_49_amount
          , ltd2.m48_number_of_donation_dollar_250_499_amount
          , ltd2.m48_number_of_donation_dollar_5_9_amount
          , ltd2.m48_number_of_donation_dollar_50_99_amount
          , ltd2.m48_number_of_donation_dollar_500_more_amount
          , ltd2.m48_number_of_list_sources
          , ltd2.m48_total_dollar_donations
          , ltd2.m48_total_number_of_donations
          , ltd2.m48_AVG_Dollar_Donation_Via_Mail
          , ltd2.m48_AVG_Dollar_Donation_Via_Mobile
          , ltd2.m48_AVG_Dollar_Donation_Via_Phone
          , ltd2.m48_AVG_Dollar_Donation_Via_Web
          , ltd2.m48_Dollar_Donation_Via_Mail
          , ltd2.m48_Dollar_Donation_Via_Mobile
          , ltd2.m48_Dollar_Donation_Via_Phone
          , ltd2.m48_Dollar_Donation_Via_Web
          , ltd2.m48_Number_Of_Donation_Dollar_AMEX
          , ltd2.m48_Number_Of_Donation_Dollar_Cash
          , ltd2.m48_Number_Of_Donation_Dollar_Check
          , ltd2.m48_Number_Of_Donation_Dollar_Master_Card
          , ltd2.m48_Number_Of_Donation_Dollar_Other
          , ltd2.m48_Number_Of_Donation_Dollar_Visa
          , ltd2.m48_Number_Of_Donation_Via_Mail
          , ltd2.m48_Number_Of_Donation_Via_Mobile
          , ltd2.m48_Number_Of_Donation_Via_Phone
          , ltd2.m48_Number_Of_Donation_Via_Web
          , ltd2.number_of_donation_april
          , ltd2.number_of_donation_august
          , ltd2.number_of_donation_december
          , ltd2.number_of_donation_february
          , ltd2.number_of_donation_january
          , ltd2.number_of_donation_july
          , ltd2.number_of_donation_june
          , ltd2.number_of_donation_march
          , ltd2.number_of_donation_may
          , ltd2.number_of_donation_november
          , ltd2.number_of_donation_october
          , ltd2.number_of_donation_september
          , ltd2.pricat_ac
          , ltd2.pricat_an
          , ltd2.pricat_cd
          , ltd2.pricat_cg
          , ltd2.pricat_ch
          , ltd2.pricat_co
          , ltd2.pricat_cs
          , ltd2.pricat_ea
          , ltd2.pricat_et
          , ltd2.pricat_hg
          , ltd2.pricat_hs
          , ltd2.pricat_li
          , ltd2.pricat_nt
          , ltd2.pricat_pa
          , ltd2.pricat_rc
          , ltd2.pricat_rd
          , ltd2.pricat_ri
          , ltd2.pricat_rj
          , ltd2.pricat_ss
          , ltd2.pricat_sw
          , ltd2.pricat_vt
          , cat2.m12_cat_ac_avg_dollar_donation
          , cat2.m12_cat_ac_dollar_donation
          , cat2.m12_cat_ac_months_since_last_donation
          , cat2.m12_cat_ac_number_of_donation
          , cat2.m12_cat_an_avg_dollar_donation
          , cat2.m12_cat_an_dollar_donation
          , cat2.m12_cat_an_months_since_last_donation
          , cat2.m12_cat_an_number_of_donation
          , cat2.m12_cat_cd_avg_dollar_donation
          , cat2.m12_cat_cd_dollar_donation
          , cat2.m12_cat_cd_months_since_last_donation
          , cat2.m12_cat_cd_number_of_donation
          , cat2.m12_cat_cg_avg_dollar_donation
          , cat2.m12_cat_cg_dollar_donation
          , cat2.m12_cat_cg_months_since_last_donation
          , cat2.m12_cat_cg_number_of_donation
          , cat2.m12_cat_ch_avg_dollar_donation
          , cat2.m12_cat_ch_dollar_donation
          , cat2.m12_cat_ch_months_since_last_donation
          , cat2.m12_cat_ch_number_of_donation
          , cat2.m12_cat_co_avg_dollar_donation
          , cat2.m12_cat_co_dollar_donation
          , cat2.m12_cat_co_months_since_last_donation
          , cat2.m12_cat_co_number_of_donation
          , cat2.m12_cat_cs_avg_dollar_donation
          , cat2.m12_cat_cs_dollar_donation
          , cat2.m12_cat_cs_months_since_last_donation
          , cat2.m12_cat_cs_number_of_donation
          , cat2.m12_cat_ea_avg_dollar_donation
          , cat2.m12_cat_ea_dollar_donation
          , cat2.m12_cat_ea_months_since_last_donation
          , cat2.m12_cat_ea_number_of_donation
          , cat2.m12_cat_ed_avg_dollar_donation
          , cat2.m12_cat_ed_dollar_donation
          , cat2.m12_cat_ed_months_since_last_donation
          , cat2.m12_cat_ed_number_of_donation
          , cat2.m12_cat_er_avg_dollar_donation
          , cat2.m12_cat_er_dollar_donation
          , cat2.m12_cat_er_months_since_last_donation
          , cat2.m12_cat_er_number_of_donation
          , cat2.m12_cat_et_avg_dollar_donation
          , cat2.m12_cat_et_dollar_donation
          , cat2.m12_cat_et_months_since_last_donation
          , cat2.m12_cat_et_number_of_donation
          , cat2.m12_cat_hg_avg_dollar_donation
          , cat2.m12_cat_hg_dollar_donation
          , cat2.m12_cat_hg_months_since_last_donation
          , cat2.m12_cat_hg_number_of_donation
          , cat2.m12_cat_hs_avg_dollar_donation
          , cat2.m12_cat_hs_dollar_donation
          , cat2.m12_cat_hs_months_since_last_donation
          , cat2.m12_cat_hs_number_of_donation
          , cat2.m12_cat_li_avg_dollar_donation
          , cat2.m12_cat_li_dollar_donation
          , cat2.m12_cat_li_months_since_last_donation
          , cat2.m12_cat_li_number_of_donation
          , cat2.m12_cat_nt_avg_dollar_donation
          , cat2.m12_cat_nt_dollar_donation
          , cat2.m12_cat_nt_months_since_last_donation
          , cat2.m12_cat_nt_number_of_donation
          , cat2.m12_cat_pa_avg_dollar_donation
          , cat2.m12_cat_pa_dollar_donation
          , cat2.m12_cat_pa_months_since_last_donation
          , cat2.m12_cat_pa_number_of_donation
          , cat2.m12_cat_rc_avg_dollar_donation
          , cat2.m12_cat_rc_dollar_donation
          , cat2.m12_cat_rc_months_since_last_donation
          , cat2.m12_cat_rc_number_of_donation
          , cat2.m12_cat_rd_avg_dollar_donation
          , cat2.m12_cat_rd_dollar_donation
          , cat2.m12_cat_rd_months_since_last_donation
          , cat2.m12_cat_rd_number_of_donation
          , cat2.m12_cat_ri_avg_dollar_donation
          , cat2.m12_cat_ri_dollar_donation
          , cat2.m12_cat_ri_months_since_last_donation
          , cat2.m12_cat_ri_number_of_donation
          , cat2.m12_cat_rj_avg_dollar_donation
          , cat2.m12_cat_rj_dollar_donation
          , cat2.m12_cat_rj_months_since_last_donation
          , cat2.m12_cat_rj_number_of_donation
          , cat2.m12_cat_sc_avg_dollar_donation
          , cat2.m12_cat_sc_dollar_donation
          , cat2.m12_cat_sc_months_since_last_donation
          , cat2.m12_cat_sc_number_of_donation
          , cat2.m12_cat_ss_avg_dollar_donation
          , cat2.m12_cat_ss_dollar_donation
          , cat2.m12_cat_ss_months_since_last_donation
          , cat2.m12_cat_ss_number_of_donation
          , cat2.m12_cat_sw_avg_dollar_donation
          , cat2.m12_cat_sw_dollar_donation
          , cat2.m12_cat_sw_months_since_last_donation
          , cat2.m12_cat_sw_number_of_donation
          , cat2.m12_cat_vt_avg_dollar_donation
          , cat2.m12_cat_vt_dollar_donation
          , cat2.m12_cat_vt_months_since_last_donation
          , cat2.m12_cat_vt_number_of_donation
          , cat2.m48_cat_ac_avg_dollar_donation
          , cat2.m48_cat_ac_dollar_donation
          , cat2.m48_cat_ac_months_since_last_donation
          , cat2.m48_cat_ac_number_of_donation
          , cat2.m48_cat_an_avg_dollar_donation
          , cat2.m48_cat_an_dollar_donation
          , cat2.m48_cat_an_months_since_last_donation
          , cat2.m48_cat_an_number_of_donation
          , cat2.m48_cat_cd_avg_dollar_donation
          , cat2.m48_cat_cd_dollar_donation
          , cat2.m48_cat_cd_months_since_last_donation
          , cat2.m48_cat_cd_number_of_donation
          , cat2.m48_cat_cg_avg_dollar_donation
          , cat2.m48_cat_cg_dollar_donation
          , cat2.m48_cat_cg_months_since_last_donation
          , cat2.m48_cat_cg_number_of_donation
          , cat2.m48_cat_ch_avg_dollar_donation
          , cat2.m48_cat_ch_dollar_donation
          , cat2.m48_cat_ch_months_since_last_donation
          , cat2.m48_cat_ch_number_of_donation
          , cat2.m48_cat_co_avg_dollar_donation
          , cat2.m48_cat_co_dollar_donation
          , cat2.m48_cat_co_months_since_last_donation
          , cat2.m48_cat_co_number_of_donation
          , cat2.m48_cat_cs_avg_dollar_donation
          , cat2.m48_cat_cs_dollar_donation
          , cat2.m48_cat_cs_months_since_last_donation
          , cat2.m48_cat_cs_number_of_donation
          , cat2.m48_cat_ea_avg_dollar_donation
          , cat2.m48_cat_ea_dollar_donation
          , cat2.m48_cat_ea_months_since_last_donation
          , cat2.m48_cat_ea_number_of_donation
          , cat2.m48_cat_ed_avg_dollar_donation
          , cat2.m48_cat_ed_dollar_donation
          , cat2.m48_cat_ed_months_since_last_donation
          , cat2.m48_cat_ed_number_of_donation
          , cat2.m48_cat_er_avg_dollar_donation
          , cat2.m48_cat_er_dollar_donation
          , cat2.m48_cat_er_months_since_last_donation
          , cat2.m48_cat_er_number_of_donation
          , cat2.m48_cat_et_avg_dollar_donation
          , cat2.m48_cat_et_dollar_donation
          , cat2.m48_cat_et_months_since_last_donation
          , cat2.m48_cat_et_number_of_donation
          , cat2.m48_cat_hg_avg_dollar_donation
          , cat2.m48_cat_hg_dollar_donation
          , cat2.m48_cat_hg_months_since_last_donation
          , cat2.m48_cat_hg_number_of_donation
          , cat2.m48_cat_hs_avg_dollar_donation
          , cat2.m48_cat_hs_dollar_donation
          , cat2.m48_cat_hs_months_since_last_donation
          , cat2.m48_cat_hs_number_of_donation
          , cat2.m48_cat_li_avg_dollar_donation
          , cat2.m48_cat_li_dollar_donation
          , cat2.m48_cat_li_months_since_last_donation
          , cat2.m48_cat_li_number_of_donation
          , cat2.m48_cat_nt_avg_dollar_donation
          , cat2.m48_cat_nt_dollar_donation
          , cat2.m48_cat_nt_months_since_last_donation
          , cat2.m48_cat_nt_number_of_donation
          , cat2.m48_cat_pa_avg_dollar_donation
          , cat2.m48_cat_pa_dollar_donation
          , cat2.m48_cat_pa_months_since_last_donation
          , cat2.m48_cat_pa_number_of_donation
          , cat2.m48_cat_rc_avg_dollar_donation
          , cat2.m48_cat_rc_dollar_donation
          , cat2.m48_cat_rc_months_since_last_donation
          , cat2.m48_cat_rc_number_of_donation
          , cat2.m48_cat_rd_avg_dollar_donation
          , cat2.m48_cat_rd_dollar_donation
          , cat2.m48_cat_rd_months_since_last_donation
          , cat2.m48_cat_rd_number_of_donation
          , cat2.m48_cat_ri_avg_dollar_donation
          , cat2.m48_cat_ri_dollar_donation
          , cat2.m48_cat_ri_months_since_last_donation
          , cat2.m48_cat_ri_number_of_donation
          , cat2.m48_cat_rj_avg_dollar_donation
          , cat2.m48_cat_rj_dollar_donation
          , cat2.m48_cat_rj_months_since_last_donation
          , cat2.m48_cat_rj_number_of_donation
          , cat2.m48_cat_sc_avg_dollar_donation
          , cat2.m48_cat_sc_dollar_donation
          , cat2.m48_cat_sc_months_since_last_donation
          , cat2.m48_cat_sc_number_of_donation
          , cat2.m48_cat_ss_avg_dollar_donation
          , cat2.m48_cat_ss_dollar_donation
          , cat2.m48_cat_ss_months_since_last_donation
          , cat2.m48_cat_ss_number_of_donation
          , cat2.m48_cat_sw_avg_dollar_donation
          , cat2.m48_cat_sw_dollar_donation
          , cat2.m48_cat_sw_months_since_last_donation
          , cat2.m48_cat_sw_number_of_donation
          , cat2.m48_cat_vt_avg_dollar_donation
	      , cat2.m48_cat_vt_dollar_donation
	      , cat2.m48_cat_vt_months_since_last_donation
	      , cat2.m48_cat_vt_number_of_donation
	      , cat2.m12_cat_tot_ac_avg_dollar_donation
	      , cat2.m12_cat_tot_ac_dollar_donation
	      , cat2.m12_cat_tot_ac_months_since_last_donation
	      , cat2.m12_cat_tot_ac_number_of_donation
	      , cat2.m12_cat_tot_an_avg_dollar_donation
	      , cat2.m12_cat_tot_an_dollar_donation
	      , cat2.m12_cat_tot_an_months_since_last_donation
	      , cat2.m12_cat_tot_an_number_of_donation
	      , cat2.m12_cat_tot_cd_avg_dollar_donation
	      , cat2.m12_cat_tot_cd_dollar_donation
	      , cat2.m12_cat_tot_cd_months_since_last_donation
	      , cat2.m12_cat_tot_cd_number_of_donation
	      , cat2.m12_cat_tot_cg_avg_dollar_donation
	      , cat2.m12_cat_tot_cg_dollar_donation
	      , cat2.m12_cat_tot_cg_months_since_last_donation
	      , cat2.m12_cat_tot_cg_number_of_donation
	      , cat2.m12_cat_tot_ch_avg_dollar_donation
	      , cat2.m12_cat_tot_ch_dollar_donation
	      , cat2.m12_cat_tot_ch_months_since_last_donation
	      , cat2.m12_cat_tot_ch_number_of_donation
	      , cat2.m12_cat_tot_co_avg_dollar_donation
	      , cat2.m12_cat_tot_co_dollar_donation
	      , cat2.m12_cat_tot_co_months_since_last_donation
	      , cat2.m12_cat_tot_co_number_of_donation
	      , cat2.m12_cat_tot_cs_avg_dollar_donation
	      , cat2.m12_cat_tot_cs_dollar_donation
	      , cat2.m12_cat_tot_cs_months_since_last_donation
	      , cat2.m12_cat_tot_cs_number_of_donation
	      , cat2.m12_cat_tot_ea_avg_dollar_donation
	      , cat2.m12_cat_tot_ea_dollar_donation
	      , cat2.m12_cat_tot_ea_months_since_last_donation
	      , cat2.m12_cat_tot_ea_number_of_donation
	      , cat2.m12_cat_tot_ed_avg_dollar_donation
	      , cat2.m12_cat_tot_ed_dollar_donation
	      , cat2.m12_cat_tot_ed_months_since_last_donation
	      , cat2.m12_cat_tot_ed_number_of_donation
	      , cat2.m12_cat_tot_er_avg_dollar_donation
	      , cat2.m12_cat_tot_er_dollar_donation
	      , cat2.m12_cat_tot_er_months_since_last_donation
	      , cat2.m12_cat_tot_er_number_of_donation
	      , cat2.m12_cat_tot_et_avg_dollar_donation
	      , cat2.m12_cat_tot_et_dollar_donation
	      , cat2.m12_cat_tot_et_months_since_last_donation
	      , cat2.m12_cat_tot_et_number_of_donation
	      , cat2.m12_cat_tot_hg_avg_dollar_donation
	      , cat2.m12_cat_tot_hg_dollar_donation
	      , cat2.m12_cat_tot_hg_months_since_last_donation
	      , cat2.m12_cat_tot_hg_number_of_donation
	      , cat2.m12_cat_tot_hs_avg_dollar_donation
	      , cat2.m12_cat_tot_hs_dollar_donation
	      , cat2.m12_cat_tot_hs_months_since_last_donation
	      , cat2.m12_cat_tot_hs_number_of_donation
	      , cat2.m12_cat_tot_li_avg_dollar_donation
	      , cat2.m12_cat_tot_li_dollar_donation
	      , cat2.m12_cat_tot_li_months_since_last_donation
	      , cat2.m12_cat_tot_li_number_of_donation
	      , cat2.m12_cat_tot_nt_avg_dollar_donation
	      , cat2.m12_cat_tot_nt_dollar_donation
	      , cat2.m12_cat_tot_nt_months_since_last_donation
	      , cat2.m12_cat_tot_nt_number_of_donation
	      , cat2.m12_cat_tot_pa_avg_dollar_donation
	      , cat2.m12_cat_tot_pa_dollar_donation
	      , cat2.m12_cat_tot_pa_months_since_last_donation
	      , cat2.m12_cat_tot_pa_number_of_donation
	      , cat2.m12_cat_tot_rc_avg_dollar_donation
	      , cat2.m12_cat_tot_rc_dollar_donation
	      , cat2.m12_cat_tot_rc_months_since_last_donation
	      , cat2.m12_cat_tot_rc_number_of_donation
	      , cat2.m12_cat_tot_rd_avg_dollar_donation
	      , cat2.m12_cat_tot_rd_dollar_donation
	      , cat2.m12_cat_tot_rd_months_since_last_donation
	      , cat2.m12_cat_tot_rd_number_of_donation
	      , cat2.m12_cat_tot_ri_avg_dollar_donation
	      , cat2.m12_cat_tot_ri_dollar_donation
	      , cat2.m12_cat_tot_ri_months_since_last_donation
	      , cat2.m12_cat_tot_ri_number_of_donation
	      , cat2.m12_cat_tot_rj_avg_dollar_donation
	      , cat2.m12_cat_tot_rj_dollar_donation
	      , cat2.m12_cat_tot_rj_months_since_last_donation
	      , cat2.m12_cat_tot_rj_number_of_donation
	      , cat2.m12_cat_tot_sc_avg_dollar_donation
	      , cat2.m12_cat_tot_sc_dollar_donation
	      , cat2.m12_cat_tot_sc_months_since_last_donation
	      , cat2.m12_cat_tot_sc_number_of_donation
	      , cat2.m12_cat_tot_ss_avg_dollar_donation
	      , cat2.m12_cat_tot_ss_dollar_donation
	      , cat2.m12_cat_tot_ss_months_since_last_donation
	      , cat2.m12_cat_tot_ss_number_of_donation
	      , cat2.m12_cat_tot_sw_avg_dollar_donation
	      , cat2.m12_cat_tot_sw_dollar_donation
	      , cat2.m12_cat_tot_sw_months_since_last_donation
	      , cat2.m12_cat_tot_sw_number_of_donation
	      , cat2.m12_cat_tot_vt_avg_dollar_donation
	      , cat2.m12_cat_tot_vt_dollar_donation
	      , cat2.m12_cat_tot_vt_months_since_last_donation
	      , cat2.m12_cat_tot_vt_number_of_donation
	      , cat2.m48_cat_tot_ac_avg_dollar_donation
	      , cat2.m48_cat_tot_ac_dollar_donation
	      , cat2.m48_cat_tot_ac_months_since_last_donation
	      , cat2.m48_cat_tot_ac_number_of_donation
	      , cat2.m48_cat_tot_an_avg_dollar_donation
	      , cat2.m48_cat_tot_an_dollar_donation
	      , cat2.m48_cat_tot_an_months_since_last_donation
	      , cat2.m48_cat_tot_an_number_of_donation
	      , cat2.m48_cat_tot_cd_avg_dollar_donation
	      , cat2.m48_cat_tot_cd_dollar_donation
	      , cat2.m48_cat_tot_cd_months_since_last_donation
	      , cat2.m48_cat_tot_cd_number_of_donation
	      , cat2.m48_cat_tot_cg_avg_dollar_donation
	      , cat2.m48_cat_tot_cg_dollar_donation
	      , cat2.m48_cat_tot_cg_months_since_last_donation
	      , cat2.m48_cat_tot_cg_number_of_donation
	      , cat2.m48_cat_tot_ch_avg_dollar_donation
	      , cat2.m48_cat_tot_ch_dollar_donation
	      , cat2.m48_cat_tot_ch_months_since_last_donation
	      , cat2.m48_cat_tot_ch_number_of_donation
	      , cat2.m48_cat_tot_co_avg_dollar_donation
	      , cat2.m48_cat_tot_co_dollar_donation
	      , cat2.m48_cat_tot_co_months_since_last_donation
	      , cat2.m48_cat_tot_co_number_of_donation
	      , cat2.m48_cat_tot_cs_avg_dollar_donation
	      , cat2.m48_cat_tot_cs_dollar_donation
	      , cat2.m48_cat_tot_cs_months_since_last_donation
	      , cat2.m48_cat_tot_cs_number_of_donation
	      , cat2.m48_cat_tot_ea_avg_dollar_donation
	      , cat2.m48_cat_tot_ea_dollar_donation
	      , cat2.m48_cat_tot_ea_months_since_last_donation
	      , cat2.m48_cat_tot_ea_number_of_donation
	      , cat2.m48_cat_tot_ed_avg_dollar_donation
	      , cat2.m48_cat_tot_ed_dollar_donation
	      , cat2.m48_cat_tot_ed_months_since_last_donation
	      , cat2.m48_cat_tot_ed_number_of_donation
	      , cat2.m48_cat_tot_er_avg_dollar_donation
	      , cat2.m48_cat_tot_er_dollar_donation
	      , cat2.m48_cat_tot_er_months_since_last_donation
	      , cat2.m48_cat_tot_er_number_of_donation
	      , cat2.m48_cat_tot_et_avg_dollar_donation
	      , cat2.m48_cat_tot_et_dollar_donation
	      , cat2.m48_cat_tot_et_months_since_last_donation
	      , cat2.m48_cat_tot_et_number_of_donation
	      , cat2.m48_cat_tot_hg_avg_dollar_donation
	      , cat2.m48_cat_tot_hg_dollar_donation
	      , cat2.m48_cat_tot_hg_months_since_last_donation
	      , cat2.m48_cat_tot_hg_number_of_donation
	      , cat2.m48_cat_tot_hs_avg_dollar_donation
	      , cat2.m48_cat_tot_hs_dollar_donation
	      , cat2.m48_cat_tot_hs_months_since_last_donation
	      , cat2.m48_cat_tot_hs_number_of_donation
	      , cat2.m48_cat_tot_li_avg_dollar_donation
	      , cat2.m48_cat_tot_li_dollar_donation
	      , cat2.m48_cat_tot_li_months_since_last_donation
	      , cat2.m48_cat_tot_li_number_of_donation
	      , cat2.m48_cat_tot_nt_avg_dollar_donation
	      , cat2.m48_cat_tot_nt_dollar_donation
	      , cat2.m48_cat_tot_nt_months_since_last_donation
	      , cat2.m48_cat_tot_nt_number_of_donation
	      , cat2.m48_cat_tot_pa_avg_dollar_donation
	      , cat2.m48_cat_tot_pa_dollar_donation
	      , cat2.m48_cat_tot_pa_months_since_last_donation
	      , cat2.m48_cat_tot_pa_number_of_donation
	      , cat2.m48_cat_tot_rc_avg_dollar_donation
	      , cat2.m48_cat_tot_rc_dollar_donation
	      , cat2.m48_cat_tot_rc_months_since_last_donation
	      , cat2.m48_cat_tot_rc_number_of_donation
	      , cat2.m48_cat_tot_rd_avg_dollar_donation
	      , cat2.m48_cat_tot_rd_dollar_donation
	      , cat2.m48_cat_tot_rd_months_since_last_donation
	      , cat2.m48_cat_tot_rd_number_of_donation
	      , cat2.m48_cat_tot_ri_avg_dollar_donation
	      , cat2.m48_cat_tot_ri_dollar_donation
	      , cat2.m48_cat_tot_ri_months_since_last_donation
	      , cat2.m48_cat_tot_ri_number_of_donation
	      , cat2.m48_cat_tot_rj_avg_dollar_donation
	      , cat2.m48_cat_tot_rj_dollar_donation
	      , cat2.m48_cat_tot_rj_months_since_last_donation
	      , cat2.m48_cat_tot_rj_number_of_donation
	      , cat2.m48_cat_tot_sc_avg_dollar_donation
	      , cat2.m48_cat_tot_sc_dollar_donation
	      , cat2.m48_cat_tot_sc_months_since_last_donation
	      , cat2.m48_cat_tot_sc_number_of_donation
	      , cat2.m48_cat_tot_ss_avg_dollar_donation
	      , cat2.m48_cat_tot_ss_dollar_donation
	      , cat2.m48_cat_tot_ss_months_since_last_donation
	      , cat2.m48_cat_tot_ss_number_of_donation
	      , cat2.m48_cat_tot_sw_avg_dollar_donation
	      , cat2.m48_cat_tot_sw_dollar_donation
	      , cat2.m48_cat_tot_sw_months_since_last_donation
	      , cat2.m48_cat_tot_sw_number_of_donation
	      , cat2.m48_cat_tot_vt_avg_dollar_donation
	      , cat2.m48_cat_tot_vt_dollar_donation
	      , cat2.m48_cat_tot_vt_months_since_last_donation
	      , cat2.m48_cat_tot_vt_number_of_donation
     from exclude_sum_ltd_np ltd2
              left outer join exclude_sum_cat_np cat2 on ltd2.company_id = cat2.company_id
              left outer join
              (
                select distinct company_id,
                                max(nvl(lems, '')) lemsmatchcode
                from {maintable_name}
                group by company_id
              )  ext392 
              on ltd2.company_id = ext392.company_id
    );

