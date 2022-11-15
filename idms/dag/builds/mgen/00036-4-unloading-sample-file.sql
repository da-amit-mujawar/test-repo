unload('
select
rtrim(tblchild.cid),
rtrim(tblchild.ap_id),
rtrim(tblchild.ap_lems),
rtrim(tblchild.company_id),
rtrim(tblchild.donormatch),
rtrim(tblchild.ltd_months_since_last_donation_date),
rtrim(tblchild.ltd_last_donation_date_yyyymm),
rtrim(tblchild.ltd_number_of_list_sources),
rtrim(tblchild.ltd_average_donation_per_transaction),
rtrim(tblchild.m12_number_of_list_sources),
rtrim(tblchild.m12_average_donation_per_transaction),
rtrim(tblchild.m12_cat_ac_number_of_donation),
rtrim(tblchild.m12_cat_an_number_of_donation),
rtrim(tblchild.m12_cat_cd_number_of_donation),
rtrim(tblchild.m12_cat_cg_number_of_donation),
rtrim(tblchild.m12_cat_ch_number_of_donation),
rtrim(tblchild.m12_cat_co_number_of_donation),
rtrim(tblchild.m12_cat_cs_number_of_donation),
rtrim(tblchild.m12_cat_ea_number_of_donation),
rtrim(tblchild.m12_cat_et_number_of_donation),
rtrim(tblchild.m12_cat_hg_number_of_donation),
rtrim(tblchild.m12_cat_hs_number_of_donation),
rtrim(tblchild.m12_cat_li_number_of_donation),
rtrim(tblchild.m12_cat_nt_number_of_donation),
rtrim(tblchild.m12_cat_pa_number_of_donation),
rtrim(tblchild.m12_cat_rc_number_of_donation),
rtrim(tblchild.m12_cat_rd_number_of_donation),
rtrim(tblchild.m12_cat_ri_number_of_donation),
rtrim(tblchild.m12_cat_rj_number_of_donation),
rtrim(tblchild.m12_cat_ss_number_of_donation),
rtrim(tblchild.m12_cat_sw_number_of_donation),
rtrim(tblchild.m12_cat_ve_number_of_donation),
rtrim(tblchild.m48_number_of_donation_dollar_cash),
rtrim(tblchild.m48_number_of_donation_dollar_check),
--rtrim(tblchild.m48_number_of_donation_dollar_visa), --fields removed as per ticket 639842
--rtrim(tblchild.m48_number_of_donation_dollar_master_card), --ticket 639842
--rtrim(tblchild.m48_number_of_donation_dollar_amex), --ticket 639842
--rtrim(tblchild.m48_number_of_donation_dollar_other), --ticket 639842
--rtrim(tblchild.m48_number_of_donation_via_mail), --ticket 639842
--rtrim(tblchild.m48_number_of_donation_via_phone), --ticket 639842
--rtrim(tblchild.m48_number_of_donation_via_web), --ticket 639842
--rtrim(tblchild.m48_number_of_donation_via_mobile), --fields removed as per ticket 639842
rtrim(tblchild.m48_cat_an_number_of_donation),
rtrim(tblchild.m48_cat_ac_number_of_donation),
rtrim(tblchild.m48_cat_cd_number_of_donation),
rtrim(tblchild.m48_cat_cs_number_of_donation),
rtrim(tblchild.m48_cat_cg_number_of_donation),
rtrim(tblchild.m48_cat_ch_number_of_donation),
rtrim(tblchild.m48_cat_co_number_of_donation),
rtrim(tblchild.m48_cat_ea_number_of_donation),
rtrim(tblchild.m48_cat_et_number_of_donation),
rtrim(tblchild.m48_cat_hg_number_of_donation),
rtrim(tblchild.m48_cat_hs_number_of_donation),
rtrim(tblchild.m48_cat_rd_number_of_donation),
rtrim(tblchild.m48_cat_ri_number_of_donation),
rtrim(tblchild.m48_cat_li_number_of_donation),
rtrim(tblchild.m48_cat_nt_number_of_donation),
rtrim(tblchild.m48_cat_pa_number_of_donation),
rtrim(tblchild.m48_cat_rc_number_of_donation),
rtrim(tblchild.m48_cat_rj_number_of_donation),
rtrim(tblchild.m48_cat_ss_number_of_donation),
rtrim(tblchild.m48_cat_ve_number_of_donation),
rtrim(tblchild.m48_cat_sw_number_of_donation),
rtrim(tblchild.m48_number_of_list_sources),
rtrim(tblchild.m48_total_number_of_donations),
rtrim(tblchild.m48_total_dollar_donations),
rtrim(tblchild.ltd_number_of_donations_0_3_months_ago),
rtrim(tblchild.ltd_number_of_donations_0_6_months_ago),
rtrim(tblchild.ltd_number_of_donations_0_12_months_ago),
rtrim(tblchild.ltd_number_of_donations_13_24_months_ago),
rtrim(tblchild.ltd_number_of_donations_over_24_months_ago)
from modelsample_{build_id}
inner join tblchild34_{build_id}_{build} tblchild
on modelsample_{build_id}.cid = tblchild.cid;
')
to 's3://{s3-internal}/neptune/mGen/ModelSample/build_{build}/mgen-50k/apogeechild'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
parallel off
CSV DELIMITER AS '|'
allowoverwrite
gzip;
