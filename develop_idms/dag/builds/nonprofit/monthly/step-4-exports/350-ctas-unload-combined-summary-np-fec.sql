/*
Combined NonProfit and FEC HH Summary
Data Source: comb_np_fec_sum, consuniv ON company_id
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
    nvl(comb_ltd_first_donation_date, ''''),
    nvl(comb_ltd_months_since_first_donation_date, 0),
    nvl(comb_ltd_months_since_last_donation_date, 0),
    nvl(comb_ltd_last_donation_date, ''''),
    nvl(comb_ltd_number_of_donations_0_3_months_ago, 0),
    nvl(comb_ltd_number_of_donations_0_6_months_ago, 0),
    nvl(comb_ltd_number_of_donations_0_12_months_ago, 0),
    nvl(comb_ltd_number_of_donations_0_24_months_ago, 0),
    nvl(comb_ltd_number_of_donations_0_36_months_ago, 0),
    nvl(comb_ltd_number_of_donations_0_48_months_ago, 0),
    nvl(comb_ltd_number_of_donations_0_96_months_ago, 0),
    nvl(comb_ltd_number_of_donations_over_96_months_ago, 0),
    nvl(comb_ltd_last_response_channel, '' ''),
    nvl(comb_ltd_months_since_last_donation_mail, 0),
    nvl(comb_ltd_months_since_last_donation_phone, 0),
    nvl(comb_ltd_months_since_last_donation_web, 0),
    nvl(comb_ltd_months_since_last_donation_mobile, 0),
    nvl(comb_ltd_highest_donation_amount_ever, 0),
    nvl(comb_ltd_lowest_donation_amount_ever, 0),
    nvl(comb_ltd_total_number_of_donations, 0),
    nvl(comb_ltd_total_dollar_donations, 0),
    nvl(comb_ltd_average_donation_per_transaction, 0),
    nvl(comb_m06_highest_donation_amount, 0),
    nvl(comb_m06_lowest_donation_amount, 0),
    nvl(comb_m06_total_number_of_donations, 0),
    nvl(comb_m06_total_dollar_donations, 0),
    nvl(comb_m06_average_donation_per_transaction, 0),
    nvl(comb_m12_highest_donation_amount, 0),
    nvl(comb_m12_lowest_donation_amount, 0),
    nvl(comb_m12_total_number_of_donations, 0),
    nvl(comb_m12_total_dollar_donations, 0),
    nvl(comb_m12_average_donation_per_transaction, 0),
    nvl(comb_m24_highest_donation_amount, 0),
    nvl(comb_m24_lowest_donation_amount, 0),
    nvl(comb_m24_total_number_of_donations, 0),
    nvl(comb_m24_total_dollar_donations, 0),
    nvl(comb_m24_average_donation_per_transaction, 0),
    nvl(comb_m48_highest_donation_amount, 0),
    nvl(comb_m48_lowest_donation_amount, 0),
    nvl(comb_m48_total_number_of_donations, 0),
    nvl(comb_m48_total_dollar_donations, 0),
    nvl(comb_m48_average_donation_per_transaction, 0)
from exclude_nonprofit_tblchild6_comb_74 c
inner join ctas_cons_unv_compid_tobedropped m on c.company_id = m.company_id and m.lems != ''''
order by c.company_id')
    to 's3://{s3-apogee}{export_mmdb_5}'
    iam_role '{iam}'
    encrypted
    cleanpath
    parallel off -- for order by clause
    gzip
    delimiter as '|'
    maxfilesize 6 gb;


insert into apogee_export_count
    (tablename, record_count)
select 'comb_np_fec_summary', count(*)
from exclude_nonprofit_tblchild6_comb_74 c
inner join ctas_cons_unv_compid_tobedropped m on c.company_id = m.company_id and m.lems != '';
*/