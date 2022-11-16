/*
 Create Apogee Child5 Table FEC Summary.
 Source is Spark Rollup Tables
 
 Update Schedule: Monthly

 --20221003 CB Remove FEC from build
*/

drop table if exists no_such_table;
/*
-- combine fec sum and cat tables as child5
begin;
    drop table if exists exclude_nonprofit_tblchild5_fec_{dbid};
    create table exclude_nonprofit_tblchild5_fec_{dbid}
    (
        id                                             bigint         encode az64,
        company_id                                     varchar(25)    ,
        pol_ltd_avg_dollar_donation                    bigint        encode az64,
        pol_ltd_first_donation_date                    varchar(8)     encode zstd,
        pol_ltd_highest_donation_amount_ever           bigint        encode az64,
        pol_ltd_last_donation_date                     varchar(8)     encode zstd,
        pol_ltd_last_donation_yyyymm                   varchar(100)   encode zstd,
        pol_ltd_lowest_donation_amount_ever            bigint        encode az64,
        pol_ltd_months_since_first_donation_date       bigint        encode az64,
        pol_ltd_months_since_last_donation_date        bigint        encode az64,
        pol_ltd_months_since_last_donation_mail        bigint        encode az64,
        pol_ltd_months_since_last_donation_mobile      bigint        encode az64,
        pol_ltd_months_since_last_donation_phone       bigint        encode az64,
        pol_ltd_months_since_last_donation_web         bigint        encode az64,
        pol_ltd_number_of_donations_0_12_months_ago    bigint        encode az64,
        pol_ltd_number_of_donations_0_24_months_ago    bigint        encode az64,
        pol_ltd_number_of_donations_0_3_months_ago     bigint        encode az64,
        pol_ltd_number_of_donations_0_36_months_ago    bigint        encode az64,
        pol_ltd_number_of_donations_0_48_months_ago    bigint        encode az64,
        pol_ltd_number_of_donations_0_6_months_ago     bigint        encode az64,
        pol_ltd_number_of_donations_0_96_months_ago    bigint        encode az64,
        pol_ltd_number_of_donations_over_96_months_ago bigint        encode az64,
        pol_ltd_total_dollar_donations                 bigint        encode az64,
        pol_ltd_total_number_of_donations              bigint        encode az64,
        pol_m06_avg_dollar_donation                    bigint        encode az64,
        pol_m06_highest_donation_amount                bigint        encode az64,
        pol_m06_lowest_donation_amount                 bigint        encode az64,
        pol_m06_total_dollar_donations                 bigint        encode az64,
        pol_m06_total_number_of_donations              bigint        encode az64,
        pol_m12_avg_dollar_donation                    bigint        encode az64,
        pol_m12_highest_donation_amount                bigint        encode az64,
        pol_m12_lowest_donation_amount                 bigint        encode az64,
        pol_m12_total_dollar_donations                 bigint        encode az64,
        pol_m12_total_number_of_donations              bigint        encode az64,
        pol_m24_avg_dollar_donation                    bigint        encode az64,
        pol_m24_highest_donation_amount                bigint        encode az64,
        pol_m24_lowest_donation_amount                 bigint        encode az64,
        pol_m24_total_dollar_donations                 bigint        encode az64,
        pol_m24_total_number_of_donations              bigint        encode az64,
        pol_m48_avg_dollar_donation                    bigint        encode az64,
        pol_m48_highest_donation_amount                bigint        encode az64,
        pol_m48_lowest_donation_amount                 bigint        encode az64,
        pol_m48_total_dollar_donations                 bigint        encode az64,
        pol_m48_total_number_of_donations              bigint        encode az64,
        pol_m48_16972_avg_dollar_donation              bigint        encode az64,
        pol_m48_16972_dollar_donation                  bigint        encode az64,
        pol_m48_16972_number_of_donation               bigint        encode az64,
        pol_m48_16972_since_last_donation              bigint        encode az64,
        pol_m48_16975_avg_dollar_donation              bigint        encode az64,
        pol_m48_16975_dollar_donation                  bigint        encode az64,
        pol_m48_16975_number_of_donation               bigint        encode az64,
        pol_m48_16975_since_last_donation              bigint        encode az64,
        pol_m48_16977_avg_dollar_donation              bigint        encode az64,
        pol_m48_16977_dollar_donation                  bigint        encode az64,
        pol_m48_16977_number_of_donation               bigint        encode az64,
        pol_m48_16977_since_last_donation              bigint        encode az64,
        pol_m48_16979_avg_dollar_donation              bigint        encode az64,
        pol_m48_16979_dollar_donation                  bigint        encode az64,
        pol_m48_16979_number_of_donation               bigint        encode az64,
        pol_m48_16979_since_last_donation              bigint        encode az64,
        pol_m48_16981_avg_dollar_donation              bigint        encode az64,
        pol_m48_16981_dollar_donation                  bigint        encode az64,
        pol_m48_16981_number_of_donation               bigint        encode az64,
        pol_m48_16981_since_last_donation              bigint        encode az64,
        pol_m48_16983_avg_dollar_donation              bigint        encode az64,
        pol_m48_16983_dollar_donation                  bigint        encode az64,
        pol_m48_16983_number_of_donation               bigint        encode az64,
        pol_m48_16983_since_last_donation              bigint        encode az64,
        pol_m48_16985_avg_dollar_donation              bigint        encode az64,
        pol_m48_16985_dollar_donation                  bigint        encode az64,
        pol_m48_16985_number_of_donation               bigint        encode az64,
        pol_m48_16985_since_last_donation              bigint        encode az64,
        pol_m48_16987_avg_dollar_donation              bigint        encode az64,
        pol_m48_16987_dollar_donation                  bigint        encode az64,
        pol_m48_16987_number_of_donation               bigint        encode az64,
        pol_m48_16987_since_last_donation              bigint        encode az64,
        pol_m48_16989_avg_dollar_donation              bigint        encode az64,
        pol_m48_16989_dollar_donation                  bigint        encode az64,
        pol_m48_16989_number_of_donation               bigint        encode az64,
        pol_m48_16989_since_last_donation              bigint        encode az64,
        pol_m48_16991_avg_dollar_donation              bigint        encode az64,
        pol_m48_16991_dollar_donation                  bigint        encode az64,
        pol_m48_16991_number_of_donation               bigint        encode az64,
        pol_m48_16991_since_last_donation              bigint        encode az64,
        pol_m48_16993_avg_dollar_donation              bigint        encode az64,
        pol_m48_16993_dollar_donation                  bigint        encode az64,
        pol_m48_16993_number_of_donation               bigint        encode az64,
        pol_m48_16993_since_last_donation              bigint        encode az64,
        pol_m48_16995_avg_dollar_donation              bigint        encode az64,
        pol_m48_16995_dollar_donation                  bigint        encode az64,
        pol_m48_16995_number_of_donation               bigint        encode az64,
        pol_m48_16995_since_last_donation              bigint        encode az64,
        pol_m48_16997_avg_dollar_donation              bigint        encode az64,
        pol_m48_16997_dollar_donation                  bigint        encode az64,
        pol_m48_16997_number_of_donation               bigint        encode az64,
        pol_m48_16997_since_last_donation              bigint        encode az64,
        pol_actblue_avg_dollar_donation                bigint        encode az64,
        pol_actblue_highest_donation_amount            bigint        encode az64,
        pol_actblue_lowest_donation_amount             bigint        encode az64,
        pol_actblue_number_of_list_sources             bigint        encode az64,
        pol_actblue_total_dollar_donations             bigint        encode az64,
        pol_actblue_total_number_of_donations          bigint        encode az64,
        pol_pricat_ar                                  bigint        encode az64,
        pol_pricat_ed                                  bigint        encode az64,
        pol_pricat_er                                  bigint        encode az64,
        pol_pricat_in                                  bigint        encode az64,
        pol_pricat_lt                                  bigint        encode az64,
        pol_pricat_ot                                  bigint        encode az64,
        pol_pricat_pr                                  bigint        encode az64,
        pol_m06_cat_ar_months_since_last_donation      bigint        encode az64,
        pol_m06_cat_ar_avg_dollar_donation             bigint        encode az64,
        pol_m06_cat_ar_number_of_donation              bigint        encode az64,
        pol_m06_cat_ar_dollar_donation                 bigint        encode az64,
        pol_m12_cat_ar_months_since_last_donation      bigint        encode az64,
        pol_m12_cat_ar_avg_dollar_donation             bigint        encode az64,
        pol_m12_cat_ar_number_of_donation              bigint        encode az64,
        pol_m12_cat_ar_dollar_donation                 bigint        encode az64,
        pol_m24_cat_ar_months_since_last_donation      bigint        encode az64,
        pol_m24_cat_ar_avg_dollar_donation             bigint        encode az64,
        pol_m24_cat_ar_number_of_donation              bigint        encode az64,
        pol_m24_cat_ar_dollar_donation                 bigint        encode az64,
        pol_m48_cat_ar_months_since_last_donation      bigint        encode az64,
        pol_m48_cat_ar_avg_dollar_donation             bigint        encode az64,
        pol_m48_cat_ar_number_of_donation              bigint        encode az64,
        pol_m48_cat_ar_dollar_donation                 bigint        encode az64,
        pol_m06_cat_ed_months_since_last_donation      bigint        encode az64,
        pol_m06_cat_ed_avg_dollar_donation             bigint        encode az64,
        pol_m06_cat_ed_number_of_donation              bigint        encode az64,
        pol_m06_cat_ed_dollar_donation                 bigint        encode az64,
        pol_m12_cat_ed_months_since_last_donation      bigint        encode az64,
        pol_m12_cat_ed_avg_dollar_donation             bigint        encode az64,
        pol_m12_cat_ed_number_of_donation              bigint        encode az64,
        pol_m12_cat_ed_dollar_donation                 bigint        encode az64,
        pol_m24_cat_ed_months_since_last_donation      bigint        encode az64,
        pol_m24_cat_ed_avg_dollar_donation             bigint        encode az64,
        pol_m24_cat_ed_number_of_donation              bigint        encode az64,
        pol_m24_cat_ed_dollar_donation                 bigint        encode az64,
        pol_m48_cat_ed_months_since_last_donation      bigint        encode az64,
        pol_m48_cat_ed_avg_dollar_donation             bigint        encode az64,
        pol_m48_cat_ed_number_of_donation              bigint        encode az64,
        pol_m48_cat_ed_dollar_donation                 bigint        encode az64,
        pol_m06_cat_er_months_since_last_donation      bigint        encode az64,
        pol_m06_cat_er_avg_dollar_donation             bigint        encode az64,
        pol_m06_cat_er_number_of_donation              bigint        encode az64,
        pol_m06_cat_er_dollar_donation                 bigint        encode az64,
        pol_m12_cat_er_months_since_last_donation      bigint        encode az64,
        pol_m12_cat_er_avg_dollar_donation             bigint        encode az64,
        pol_m12_cat_er_number_of_donation              bigint        encode az64,
        pol_m12_cat_er_dollar_donation                 bigint        encode az64,
        pol_m24_cat_er_months_since_last_donation      bigint        encode az64,
        pol_m24_cat_er_avg_dollar_donation             bigint        encode az64,
        pol_m24_cat_er_number_of_donation              bigint        encode az64,
        pol_m24_cat_er_dollar_donation                 bigint        encode az64,
        pol_m48_cat_er_months_since_last_donation      bigint        encode az64,
        pol_m48_cat_er_avg_dollar_donation             bigint        encode az64,
        pol_m48_cat_er_number_of_donation              bigint        encode az64,
        pol_m48_cat_er_dollar_donation                 bigint        encode az64,
        pol_m06_cat_in_months_since_last_donation      bigint        encode az64,
        pol_m06_cat_in_avg_dollar_donation             bigint        encode az64,
        pol_m06_cat_in_number_of_donation              bigint        encode az64,
        pol_m06_cat_in_dollar_donation                 bigint        encode az64,
        pol_m12_cat_in_months_since_last_donation      bigint        encode az64,
        pol_m12_cat_in_avg_dollar_donation             bigint        encode az64,
        pol_m12_cat_in_number_of_donation              bigint        encode az64,
        pol_m12_cat_in_dollar_donation                 bigint        encode az64,
        pol_m24_cat_in_months_since_last_donation      bigint        encode az64,
        pol_m24_cat_in_avg_dollar_donation             bigint        encode az64,
        pol_m24_cat_in_number_of_donation              bigint        encode az64,
        pol_m24_cat_in_dollar_donation                 bigint        encode az64,
        pol_m48_cat_in_months_since_last_donation      bigint        encode az64,
        pol_m48_cat_in_avg_dollar_donation             bigint        encode az64,
        pol_m48_cat_in_number_of_donation              bigint        encode az64,
        pol_m48_cat_in_dollar_donation                 bigint        encode az64,
        pol_m06_cat_lt_months_since_last_donation      bigint        encode az64,
        pol_m06_cat_lt_avg_dollar_donation             bigint        encode az64,
        pol_m06_cat_lt_number_of_donation              bigint        encode az64,
        pol_m06_cat_lt_dollar_donation                 bigint        encode az64,
        pol_m12_cat_lt_months_since_last_donation      bigint        encode az64,
        pol_m12_cat_lt_avg_dollar_donation             bigint        encode az64,
        pol_m12_cat_lt_number_of_donation              bigint        encode az64,
        pol_m12_cat_lt_dollar_donation                 bigint        encode az64,
        pol_m24_cat_lt_months_since_last_donation      bigint        encode az64,
        pol_m24_cat_lt_avg_dollar_donation             bigint        encode az64,
        pol_m24_cat_lt_number_of_donation              bigint        encode az64,
        pol_m24_cat_lt_dollar_donation                 bigint        encode az64,
        pol_m48_cat_lt_months_since_last_donation      bigint        encode az64,
        pol_m48_cat_lt_avg_dollar_donation             bigint        encode az64,
        pol_m48_cat_lt_number_of_donation              bigint        encode az64,
        pol_m48_cat_lt_dollar_donation                 bigint        encode az64,
        pol_m06_cat_ot_months_since_last_donation      bigint        encode az64,
        pol_m06_cat_ot_avg_dollar_donation             bigint        encode az64,
        pol_m06_cat_ot_number_of_donation              bigint        encode az64,
        pol_m06_cat_ot_dollar_donation                 bigint        encode az64,
        pol_m12_cat_ot_months_since_last_donation      bigint        encode az64,
        pol_m12_cat_ot_avg_dollar_donation             bigint        encode az64,
        pol_m12_cat_ot_number_of_donation              bigint        encode az64,
        pol_m12_cat_ot_dollar_donation                 bigint        encode az64,
        pol_m24_cat_ot_months_since_last_donation      bigint        encode az64,
        pol_m24_cat_ot_avg_dollar_donation             bigint        encode az64,
        pol_m24_cat_ot_number_of_donation              bigint        encode az64,
        pol_m24_cat_ot_dollar_donation                 bigint        encode az64,
        pol_m48_cat_ot_months_since_last_donation      bigint        encode az64,
        pol_m48_cat_ot_avg_dollar_donation             bigint        encode az64,
        pol_m48_cat_ot_number_of_donation              bigint        encode az64,
        pol_m48_cat_ot_dollar_donation                 bigint        encode az64,
        pol_m06_cat_pr_months_since_last_donation      bigint        encode az64,
        pol_m06_cat_pr_avg_dollar_donation             bigint        encode az64,
        pol_m06_cat_pr_number_of_donation              bigint        encode az64,
        pol_m06_cat_pr_dollar_donation                 bigint        encode az64,
        pol_m12_cat_pr_months_since_last_donation      bigint        encode az64,
        pol_m12_cat_pr_avg_dollar_donation             bigint        encode az64,
        pol_m12_cat_pr_number_of_donation              bigint        encode az64,
        pol_m12_cat_pr_dollar_donation                 bigint        encode az64,
        pol_m24_cat_pr_months_since_last_donation      bigint        encode az64,
        pol_m24_cat_pr_avg_dollar_donation             bigint        encode az64,
        pol_m24_cat_pr_number_of_donation              bigint        encode az64,
        pol_m24_cat_pr_dollar_donation                 bigint        encode az64,
        pol_m48_cat_pr_months_since_last_donation      bigint        encode az64,
        pol_m48_cat_pr_avg_dollar_donation             bigint        encode az64,
        pol_m48_cat_pr_number_of_donation              bigint        encode az64,
        pol_m48_cat_pr_dollar_donation                 bigint        encode az64
    )
        diststyle key
        distkey (company_id)
        sortkey (company_id)
    ;
end;

begin;
    insert into exclude_nonprofit_tblchild5_fec_{dbid}
        (select ltd.id
              , ltd.company_id
              , ltd.pol_ltd_avg_dollar_donation
              , ltd.pol_ltd_first_donation_date
              , ltd.pol_ltd_highest_donation_amount_ever
              , ltd.pol_ltd_last_donation_date
              , ltd.pol_ltd_last_donation_yyyymm
              , ltd.pol_ltd_lowest_donation_amount_ever
              , ltd.pol_ltd_months_since_first_donation_date
              , ltd.pol_ltd_months_since_last_donation_date
              , ltd.pol_ltd_months_since_last_donation_mail
              , ltd.pol_ltd_months_since_last_donation_mobile
              , ltd.pol_ltd_months_since_last_donation_phone
              , ltd.pol_ltd_months_since_last_donation_web
              , ltd.pol_ltd_number_of_donations_0_12_months_ago
              , ltd.pol_ltd_number_of_donations_0_24_months_ago
              , ltd.pol_ltd_number_of_donations_0_3_months_ago
              , ltd.pol_ltd_number_of_donations_0_36_months_ago
              , ltd.pol_ltd_number_of_donations_0_48_months_ago
              , ltd.pol_ltd_number_of_donations_0_6_months_ago
              , ltd.pol_ltd_number_of_donations_0_96_months_ago
              , ltd.pol_ltd_number_of_donations_over_96_months_ago
              , ltd.pol_ltd_total_dollar_donations
              , ltd.pol_ltd_total_number_of_donations
              , ltd.pol_m06_avg_dollar_donation
              , ltd.pol_m06_highest_donation_amount
              , ltd.pol_m06_lowest_donation_amount
              , ltd.pol_m06_total_dollar_donations
              , ltd.pol_m06_total_number_of_donations
              , ltd.pol_m12_avg_dollar_donation
              , ltd.pol_m12_highest_donation_amount
              , ltd.pol_m12_lowest_donation_amount
              , ltd.pol_m12_total_dollar_donations
              , ltd.pol_m12_total_number_of_donations
              , ltd.pol_m24_avg_dollar_donation
              , ltd.pol_m24_highest_donation_amount
              , ltd.pol_m24_lowest_donation_amount
              , ltd.pol_m24_total_dollar_donations
              , ltd.pol_m24_total_number_of_donations
              , ltd.pol_m48_avg_dollar_donation
              , ltd.pol_m48_highest_donation_amount
              , ltd.pol_m48_lowest_donation_amount
              , ltd.pol_m48_total_dollar_donations
              , ltd.pol_m48_total_number_of_donations
              , ltd.pol_m48_16972_avg_dollar_donation
              , ltd.pol_m48_16972_dollar_donation
              , ltd.pol_m48_16972_number_of_donation
              , ltd.pol_m48_16972_since_last_donation
              , ltd.pol_m48_16975_avg_dollar_donation
              , ltd.pol_m48_16975_dollar_donation
              , ltd.pol_m48_16975_number_of_donation
              , ltd.pol_m48_16975_since_last_donation
              , ltd.pol_m48_16977_avg_dollar_donation
              , ltd.pol_m48_16977_dollar_donation
              , ltd.pol_m48_16977_number_of_donation
              , ltd.pol_m48_16977_since_last_donation
              , ltd.pol_m48_16979_avg_dollar_donation
              , ltd.pol_m48_16979_dollar_donation
              , ltd.pol_m48_16979_number_of_donation
              , ltd.pol_m48_16979_since_last_donation
              , ltd.pol_m48_16981_avg_dollar_donation
              , ltd.pol_m48_16981_dollar_donation
              , ltd.pol_m48_16981_number_of_donation
              , ltd.pol_m48_16981_since_last_donation
              , ltd.pol_m48_16983_avg_dollar_donation
              , ltd.pol_m48_16983_dollar_donation
              , ltd.pol_m48_16983_number_of_donation
              , ltd.pol_m48_16983_since_last_donation
              , ltd.pol_m48_16985_avg_dollar_donation
              , ltd.pol_m48_16985_dollar_donation
              , ltd.pol_m48_16985_number_of_donation
              , ltd.pol_m48_16985_since_last_donation
              , ltd.pol_m48_16987_avg_dollar_donation
              , ltd.pol_m48_16987_dollar_donation
              , ltd.pol_m48_16987_number_of_donation
              , ltd.pol_m48_16987_since_last_donation
              , ltd.pol_m48_16989_avg_dollar_donation
              , ltd.pol_m48_16989_dollar_donation
              , ltd.pol_m48_16989_number_of_donation
              , ltd.pol_m48_16989_since_last_donation
              , ltd.pol_m48_16991_avg_dollar_donation
              , ltd.pol_m48_16991_dollar_donation
              , ltd.pol_m48_16991_number_of_donation
              , ltd.pol_m48_16991_since_last_donation
              , ltd.pol_m48_16993_avg_dollar_donation
              , ltd.pol_m48_16993_dollar_donation
              , ltd.pol_m48_16993_number_of_donation
              , ltd.pol_m48_16993_since_last_donation
              , ltd.pol_m48_16995_avg_dollar_donation
              , ltd.pol_m48_16995_dollar_donation
              , ltd.pol_m48_16995_number_of_donation
              , ltd.pol_m48_16995_since_last_donation
              , ltd.pol_m48_16997_avg_dollar_donation
              , ltd.pol_m48_16997_dollar_donation
              , ltd.pol_m48_16997_number_of_donation
              , ltd.pol_m48_16997_since_last_donation
              , ltd.pol_actblue_avg_dollar_donation
              , ltd.pol_actblue_highest_donation_amount
              , ltd.pol_actblue_lowest_donation_amount
              , ltd.pol_actblue_number_of_list_sources
              , ltd.pol_actblue_total_dollar_donations
              , ltd.pol_actblue_total_number_of_donations
              , ltd.pol_pricat_ar
              , ltd.pol_pricat_ed
              , ltd.pol_pricat_er
              , ltd.pol_pricat_in
              , ltd.pol_pricat_lt
              , ltd.pol_pricat_ot
              , ltd.pol_pricat_pr
              , cat.pol_m06_cat_ar_months_since_last_donation
              , cat.pol_m06_cat_ar_avg_dollar_donation
              , cat.pol_m06_cat_ar_number_of_donation
              , cat.pol_m06_cat_ar_dollar_donation
              , cat.pol_m12_cat_ar_months_since_last_donation
              , cat.pol_m12_cat_ar_avg_dollar_donation
              , cat.pol_m12_cat_ar_number_of_donation
              , cat.pol_m12_cat_ar_dollar_donation
              , cat.pol_m24_cat_ar_months_since_last_donation
              , cat.pol_m24_cat_ar_avg_dollar_donation
              , cat.pol_m24_cat_ar_number_of_donation
              , cat.pol_m24_cat_ar_dollar_donation
              , cat.pol_m48_cat_ar_months_since_last_donation
              , cat.pol_m48_cat_ar_avg_dollar_donation
              , cat.pol_m48_cat_ar_number_of_donation
              , cat.pol_m48_cat_ar_dollar_donation
              , cat.pol_m06_cat_ed_months_since_last_donation
              , cat.pol_m06_cat_ed_avg_dollar_donation
              , cat.pol_m06_cat_ed_number_of_donation
              , cat.pol_m06_cat_ed_dollar_donation
              , cat.pol_m12_cat_ed_months_since_last_donation
              , cat.pol_m12_cat_ed_avg_dollar_donation
              , cat.pol_m12_cat_ed_number_of_donation
              , cat.pol_m12_cat_ed_dollar_donation
              , cat.pol_m24_cat_ed_months_since_last_donation
              , cat.pol_m24_cat_ed_avg_dollar_donation
              , cat.pol_m24_cat_ed_number_of_donation
              , cat.pol_m24_cat_ed_dollar_donation
              , cat.pol_m48_cat_ed_months_since_last_donation
              , cat.pol_m48_cat_ed_avg_dollar_donation
              , cat.pol_m48_cat_ed_number_of_donation
              , cat.pol_m48_cat_ed_dollar_donation
              , cat.pol_m06_cat_er_months_since_last_donation
              , cat.pol_m06_cat_er_avg_dollar_donation
              , cat.pol_m06_cat_er_number_of_donation
              , cat.pol_m06_cat_er_dollar_donation
              , cat.pol_m12_cat_er_months_since_last_donation
              , cat.pol_m12_cat_er_avg_dollar_donation
              , cat.pol_m12_cat_er_number_of_donation
              , cat.pol_m12_cat_er_dollar_donation
              , cat.pol_m24_cat_er_months_since_last_donation
              , cat.pol_m24_cat_er_avg_dollar_donation
              , cat.pol_m24_cat_er_number_of_donation
              , cat.pol_m24_cat_er_dollar_donation
              , cat.pol_m48_cat_er_months_since_last_donation
              , cat.pol_m48_cat_er_avg_dollar_donation
              , cat.pol_m48_cat_er_number_of_donation
              , cat.pol_m48_cat_er_dollar_donation
              , cat.pol_m06_cat_in_months_since_last_donation
              , cat.pol_m06_cat_in_avg_dollar_donation
              , cat.pol_m06_cat_in_number_of_donation
              , cat.pol_m06_cat_in_dollar_donation
              , cat.pol_m12_cat_in_months_since_last_donation
              , cat.pol_m12_cat_in_avg_dollar_donation
              , cat.pol_m12_cat_in_number_of_donation
              , cat.pol_m12_cat_in_dollar_donation
              , cat.pol_m24_cat_in_months_since_last_donation
              , cat.pol_m24_cat_in_avg_dollar_donation
              , cat.pol_m24_cat_in_number_of_donation
              , cat.pol_m24_cat_in_dollar_donation
              , cat.pol_m48_cat_in_months_since_last_donation
              , cat.pol_m48_cat_in_avg_dollar_donation
              , cat.pol_m48_cat_in_number_of_donation
              , cat.pol_m48_cat_in_dollar_donation
              , cat.pol_m06_cat_lt_months_since_last_donation
              , cat.pol_m06_cat_lt_avg_dollar_donation
              , cat.pol_m06_cat_lt_number_of_donation
              , cat.pol_m06_cat_lt_dollar_donation
              , cat.pol_m12_cat_lt_months_since_last_donation
              , cat.pol_m12_cat_lt_avg_dollar_donation
              , cat.pol_m12_cat_lt_number_of_donation
              , cat.pol_m12_cat_lt_dollar_donation
              , cat.pol_m24_cat_lt_months_since_last_donation
              , cat.pol_m24_cat_lt_avg_dollar_donation
              , cat.pol_m24_cat_lt_number_of_donation
              , cat.pol_m24_cat_lt_dollar_donation
              , cat.pol_m48_cat_lt_months_since_last_donation
              , cat.pol_m48_cat_lt_avg_dollar_donation
              , cat.pol_m48_cat_lt_number_of_donation
              , cat.pol_m48_cat_lt_dollar_donation
              , cat.pol_m06_cat_ot_months_since_last_donation
              , cat.pol_m06_cat_ot_avg_dollar_donation
              , cat.pol_m06_cat_ot_number_of_donation
              , cat.pol_m06_cat_ot_dollar_donation
              , cat.pol_m12_cat_ot_months_since_last_donation
              , cat.pol_m12_cat_ot_avg_dollar_donation
              , cat.pol_m12_cat_ot_number_of_donation
              , cat.pol_m12_cat_ot_dollar_donation
              , cat.pol_m24_cat_ot_months_since_last_donation
              , cat.pol_m24_cat_ot_avg_dollar_donation
              , cat.pol_m24_cat_ot_number_of_donation
              , cat.pol_m24_cat_ot_dollar_donation
              , cat.pol_m48_cat_ot_months_since_last_donation
              , cat.pol_m48_cat_ot_avg_dollar_donation
              , cat.pol_m48_cat_ot_number_of_donation
              , cat.pol_m48_cat_ot_dollar_donation
              , cat.pol_m06_cat_pr_months_since_last_donation
              , cat.pol_m06_cat_pr_avg_dollar_donation
              , cat.pol_m06_cat_pr_number_of_donation
              , cat.pol_m06_cat_pr_dollar_donation
              , cat.pol_m12_cat_pr_months_since_last_donation
              , cat.pol_m12_cat_pr_avg_dollar_donation
              , cat.pol_m12_cat_pr_number_of_donation
              , cat.pol_m12_cat_pr_dollar_donation
              , cat.pol_m24_cat_pr_months_since_last_donation
              , cat.pol_m24_cat_pr_avg_dollar_donation
              , cat.pol_m24_cat_pr_number_of_donation
              , cat.pol_m24_cat_pr_dollar_donation
              , cat.pol_m48_cat_pr_months_since_last_donation
              , cat.pol_m48_cat_pr_avg_dollar_donation
              , cat.pol_m48_cat_pr_number_of_donation
              , cat.pol_m48_cat_pr_dollar_donation
         from exclude_sum_fec ltd
              left outer join exclude_sum_cat_fec cat on ltd.company_id = cat.company_id);

end;

*/


