/*
FEC Donor HH Summary
Data Source: fec_sum, consuniv ON company_id
Condition: unique company_id
OrderBy: company_id
Other: pass 0 for nulls

 Update Schedule: Monthly


 --20221003 CB Remove FEC from build

*/
drop table if exists no_such_table;
/*
unload ('select c.company_id,
        nvl(lems, ''''),
        nvl(pol_ltd_first_donation_date, ''''),
        nvl(pol_ltd_last_donation_date, ''''),
        nvl(pol_ltd_highest_donation_amount_ever, 0),
        nvl(pol_ltd_lowest_donation_amount_ever, 0),
        nvl(pol_ltd_total_number_of_donations, 0),
        nvl(pol_ltd_total_dollar_donations, 0),
        nvl(pol_ltd_avg_dollar_donation, 0),
        nvl(pol_ltd_number_of_donations_0_3_months_ago, 0),
        nvl(pol_ltd_number_of_donations_0_6_months_ago, 0),
        nvl(pol_ltd_number_of_donations_0_12_months_ago, 0),
        nvl(pol_ltd_number_of_donations_0_24_months_ago, 0),
        nvl(pol_ltd_number_of_donations_0_36_months_ago, 0),
        nvl(pol_ltd_number_of_donations_0_48_months_ago, 0),
        nvl(pol_ltd_number_of_donations_0_96_months_ago, 0),
        nvl(pol_ltd_number_of_donations_over_96_months_ago, 0),
        nvl(pol_ltd_months_since_first_donation_date, 0),
        nvl(pol_ltd_months_since_last_donation_date, 0),
        nvl(pol_ltd_months_since_last_donation_mail, 0),
        nvl(pol_ltd_months_since_last_donation_mobile, 0),
        nvl(pol_ltd_months_since_last_donation_phone, 0),
        nvl(pol_ltd_months_since_last_donation_web, 0),
        nvl(pol_m06_highest_donation_amount, 0),
        nvl(pol_m06_lowest_donation_amount, 0),
        nvl(pol_m06_total_number_of_donations, 0),
        nvl(pol_m06_total_dollar_donations, 0),
        nvl(pol_m06_avg_dollar_donation, 0),
        nvl(pol_m12_highest_donation_amount, 0),
        nvl(pol_m12_lowest_donation_amount, 0),
        nvl(pol_m12_total_number_of_donations, 0),
        nvl(pol_m12_total_dollar_donations, 0),
        nvl(pol_m12_avg_dollar_donation, 0),
        nvl(pol_m24_highest_donation_amount, 0),
        nvl(pol_m24_lowest_donation_amount, 0),
        nvl(pol_m24_total_number_of_donations, 0),
        nvl(pol_m24_total_dollar_donations, 0),
        nvl(pol_m24_avg_dollar_donation, 0),
        nvl(pol_m48_highest_donation_amount, 0),
        nvl(pol_m48_lowest_donation_amount, 0),
        nvl(pol_m48_total_number_of_donations, 0),
        nvl(pol_m48_total_dollar_donations, 0),
        nvl(pol_m48_avg_dollar_donation, 0),
        nvl(pol_m06_cat_ar_avg_dollar_donation, 0),
        nvl(pol_m06_cat_ed_avg_dollar_donation, 0),
        nvl(pol_m06_cat_er_avg_dollar_donation, 0),
        nvl(pol_m06_cat_in_avg_dollar_donation, 0),
        nvl(pol_m06_cat_lt_avg_dollar_donation, 0),
        nvl(pol_m06_cat_ot_avg_dollar_donation, 0),
        nvl(pol_m06_cat_pr_avg_dollar_donation, 0),
        nvl(pol_m06_cat_ar_months_since_last_donation, 0),
        nvl(pol_m06_cat_ed_months_since_last_donation, 0),
        nvl(pol_m06_cat_er_months_since_last_donation, 0),
        nvl(pol_m06_cat_in_months_since_last_donation, 0),
        nvl(pol_m06_cat_lt_months_since_last_donation, 0),
        nvl(pol_m06_cat_ot_months_since_last_donation, 0),
        nvl(pol_m06_cat_pr_months_since_last_donation, 0),
        nvl(pol_m06_cat_ar_number_of_donation, 0),
        nvl(pol_m06_cat_ed_number_of_donation, 0),
        nvl(pol_m06_cat_er_number_of_donation, 0),
        nvl(pol_m06_cat_in_number_of_donation, 0),
        nvl(pol_m06_cat_lt_number_of_donation, 0),
        nvl(pol_m06_cat_ot_number_of_donation, 0),
        nvl(pol_m06_cat_pr_number_of_donation, 0),
        nvl(pol_m06_cat_ar_dollar_donation, 0),
        nvl(pol_m06_cat_ed_dollar_donation, 0),
        nvl(pol_m06_cat_er_dollar_donation, 0),
        nvl(pol_m06_cat_in_dollar_donation, 0),
        nvl(pol_m06_cat_lt_dollar_donation, 0),
        nvl(pol_m06_cat_ot_dollar_donation, 0),
        nvl(pol_m06_cat_pr_dollar_donation, 0),
        nvl(pol_m12_cat_ar_avg_dollar_donation, 0),
        nvl(pol_m12_cat_ed_avg_dollar_donation, 0),
        nvl(pol_m12_cat_er_avg_dollar_donation, 0),
        nvl(pol_m12_cat_in_avg_dollar_donation, 0),
        nvl(pol_m12_cat_lt_avg_dollar_donation, 0),
        nvl(pol_m12_cat_ot_avg_dollar_donation, 0),
        nvl(pol_m12_cat_pr_avg_dollar_donation, 0),
        nvl(pol_m12_cat_ar_months_since_last_donation, 0),
        nvl(pol_m12_cat_ed_months_since_last_donation, 0),
        nvl(pol_m12_cat_er_months_since_last_donation, 0),
        nvl(pol_m12_cat_in_months_since_last_donation, 0),
        nvl(pol_m12_cat_lt_months_since_last_donation, 0),
        nvl(pol_m12_cat_ot_months_since_last_donation, 0),
        nvl(pol_m12_cat_pr_months_since_last_donation, 0),
        nvl(pol_m12_cat_ar_number_of_donation, 0),
        nvl(pol_m12_cat_ed_number_of_donation, 0),
        nvl(pol_m12_cat_er_number_of_donation, 0),
        nvl(pol_m12_cat_in_number_of_donation, 0),
        nvl(pol_m12_cat_lt_number_of_donation, 0),
        nvl(pol_m12_cat_ot_number_of_donation, 0),
        nvl(pol_m12_cat_pr_number_of_donation, 0),
        nvl(pol_m12_cat_ar_dollar_donation, 0),
        nvl(pol_m12_cat_ed_dollar_donation, 0),
        nvl(pol_m12_cat_er_dollar_donation, 0),
        nvl(pol_m12_cat_in_dollar_donation, 0),
        nvl(pol_m12_cat_lt_dollar_donation, 0),
        nvl(pol_m12_cat_ot_dollar_donation, 0),
        nvl(pol_m12_cat_pr_dollar_donation, 0),
        nvl(pol_m24_cat_ar_avg_dollar_donation, 0),
        nvl(pol_m24_cat_ed_avg_dollar_donation, 0),
        nvl(pol_m24_cat_er_avg_dollar_donation, 0),
        nvl(pol_m24_cat_in_avg_dollar_donation, 0),
        nvl(pol_m24_cat_lt_avg_dollar_donation, 0),
        nvl(pol_m24_cat_ot_avg_dollar_donation, 0),
        nvl(pol_m24_cat_pr_avg_dollar_donation, 0),
        nvl(pol_m24_cat_ar_months_since_last_donation, 0),
        nvl(pol_m24_cat_ed_months_since_last_donation, 0),
        nvl(pol_m24_cat_er_months_since_last_donation, 0),
        nvl(pol_m24_cat_in_months_since_last_donation, 0),
        nvl(pol_m24_cat_lt_months_since_last_donation, 0),
        nvl(pol_m24_cat_ot_months_since_last_donation, 0),
        nvl(pol_m24_cat_pr_months_since_last_donation, 0),
        nvl(pol_m24_cat_ar_number_of_donation, 0),
        nvl(pol_m24_cat_ed_number_of_donation, 0),
        nvl(pol_m24_cat_er_number_of_donation, 0),
        nvl(pol_m24_cat_in_number_of_donation, 0),
        nvl(pol_m24_cat_lt_number_of_donation, 0),
        nvl(pol_m24_cat_ot_number_of_donation, 0),
        nvl(pol_m24_cat_pr_number_of_donation, 0),
        nvl(pol_m24_cat_ar_dollar_donation, 0),
        nvl(pol_m24_cat_ed_dollar_donation, 0),
        nvl(pol_m24_cat_er_dollar_donation, 0),
        nvl(pol_m24_cat_in_dollar_donation, 0),
        nvl(pol_m24_cat_lt_dollar_donation, 0),
        nvl(pol_m24_cat_ot_dollar_donation, 0),
        nvl(pol_m24_cat_pr_dollar_donation, 0),
        nvl(pol_m48_cat_ar_avg_dollar_donation, 0),
        nvl(pol_m48_cat_ed_avg_dollar_donation, 0),
        nvl(pol_m48_cat_er_avg_dollar_donation, 0),
        nvl(pol_m48_cat_in_avg_dollar_donation, 0),
        nvl(pol_m48_cat_lt_avg_dollar_donation, 0),
        nvl(pol_m48_cat_ot_avg_dollar_donation, 0),
        nvl(pol_m48_cat_pr_avg_dollar_donation, 0),
        nvl(pol_m48_cat_ar_months_since_last_donation, 0),
        nvl(pol_m48_cat_ed_months_since_last_donation, 0),
        nvl(pol_m48_cat_er_months_since_last_donation, 0),
        nvl(pol_m48_cat_in_months_since_last_donation, 0),
        nvl(pol_m48_cat_lt_months_since_last_donation, 0),
        nvl(pol_m48_cat_ot_months_since_last_donation, 0),
        nvl(pol_m48_cat_pr_months_since_last_donation, 0),
        nvl(pol_m48_cat_ar_number_of_donation, 0),
        nvl(pol_m48_cat_ed_number_of_donation, 0),
        nvl(pol_m48_cat_er_number_of_donation, 0),
        nvl(pol_m48_cat_in_number_of_donation, 0),
        nvl(pol_m48_cat_lt_number_of_donation, 0),
        nvl(pol_m48_cat_ot_number_of_donation, 0),
        nvl(pol_m48_cat_pr_number_of_donation, 0),
        nvl(pol_m48_cat_ar_dollar_donation, 0),
        nvl(pol_m48_cat_ed_dollar_donation, 0),
        nvl(pol_m48_cat_er_dollar_donation, 0),
        nvl(pol_m48_cat_in_dollar_donation, 0),
        nvl(pol_m48_cat_lt_dollar_donation, 0),
        nvl(pol_m48_cat_ot_dollar_donation, 0),
        nvl(pol_m48_cat_pr_dollar_donation, 0),
        nvl(pol_actblue_highest_donation_amount, 0),
        nvl(pol_actblue_lowest_donation_amount, 0),
        nvl(pol_actblue_total_number_of_donations, 0),
        nvl(pol_actblue_total_dollar_donations, 0),
        nvl(pol_actblue_avg_dollar_donation, 0),
        nvl(pol_actblue_number_of_list_sources, 0)
from exclude_nonprofit_tblchild5_fec_74 c
    inner join ctas_cons_unv_compid_tobedropped m on c.company_id = m.company_id and m.lems != ''''
order by c.company_id')
    to 's3://{s3-apogee}{export_mmdb_4}'
    iam_role '{iam}'
    encrypted
    cleanpath
    parallel off -- for order by clause
    gzip
    delimiter as '|'
    maxfilesize 6 gb;

insert into apogee_export_count
    (tablename, record_count)
select 'fec_hh_summary', count(*)
from exclude_nonprofit_tblchild5_fec_74 c
    inner join ctas_cons_unv_compid_tobedropped m on c.company_id = m.company_id and m.lems != '';
*/