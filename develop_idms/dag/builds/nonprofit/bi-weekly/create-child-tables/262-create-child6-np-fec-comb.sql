/*
 Create Apogee Child6 Table Step 1 of 4 - Combined Summary with Child1 + Child5.
 Create Apogee Child6 Table Step 2 of 4 - Combined Summary with Child1 + Child5.
 Create Apogee Child6 Table Step 4 of 4 - Combined Summary with Child1 + Child5.

 --20221003 CB Remove FEC from build
*/


-- combine transaction detail tables nonprofit (child3) + fec (child4)
drop table if exists no_such_table;       --ctas_trans_comb_tobedropped;
/*
create table ctas_trans_comb_tobedropped
(
    company_id             varchar(25),
    detail_donationdate    varchar(8),
    detail_donationdollar  int,
    detail_donationchannel char(1)
);

insert into ctas_trans_comb_tobedropped
(company_id,
    detail_donationdate,
    detail_donationdollar,
    detail_donationchannel)
select company_id,
        detail_donationdate,
        detail_donationdollar,
        detail_donationchannel
from exclude_nonprofit_transactions_{dbid};

insert into ctas_trans_comb_tobedropped (company_id,
                                            detail_donationdate,
                                            detail_donationdollar,
                                            detail_donationchannel)
select company_id,
        detail_donationdate,
        detail_donationdollar,
        detail_donationchannel
from exclude_nonprofit_transactions_fec_{dbid};


-- create distinct company_id for combined summaries
drop table if exists ctas_companyid_tobedropped;
create table ctas_companyid_tobedropped
(
    id                                              bigint identity,
    company_id                                      varchar(25)
);

insert into ctas_companyid_tobedropped (company_id)
select distinct company_id
from ctas_trans_comb_tobedropped;


begin;
    drop table if exists ctas_donation_tobedropped;
    create table ctas_donation_tobedropped
    as
    select min(detail_donationdate)       comb_ltd_first_donation_date,
           max(detail_donationdate)       comb_ltd_last_donation_date,
           max(detail_donationdollar)     comb_ltd_highest_donation_amount_ever,
           min(detail_donationdollar)     comb_ltd_lowest_donation_amount_ever,
           count(*)                       comb_ltd_total_number_of_donations,
           sum(nps.detail_donationdollar) comb_ltd_total_dollar_donations,
           avg(nps.detail_donationdollar) comb_ltd_average_donation_per_transaction,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) between 0 and 3 then 1
                   else 0 end)            comb_ltd_number_of_donations_0_3_months_ago,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) between 0 and 6 then 1
                   else 0 end)            comb_ltd_number_of_donations_0_6_months_ago,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) between 0 and 12 then 1
                   else 0 end)            comb_ltd_number_of_donations_0_12_months_ago,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) between 0 and 24 then 1
                   else 0 end)            comb_ltd_number_of_donations_0_24_months_ago,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) between 0 and 36 then 1
                   else 0 end)            comb_ltd_number_of_donations_0_36_months_ago,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) between 0 and 48 then 1
                   else 0 end)            comb_ltd_number_of_donations_0_48_months_ago,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) between 0 and 96 then 1
                   else 0 end)            comb_ltd_number_of_donations_0_96_months_ago,
           sum(case
                   when datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) >= 96 then 1
                   else 0 end)            comb_ltd_number_of_donations_over_96_months_ago,
           company_id
    from ctas_trans_comb_tobedropped nps
    group by company_id;
end;


drop table if exists ctas_channelsummary_tobedropped;
create table ctas_channelsummary_tobedropped
as
select company_id,
       detail_donationchannel,
       case
           when detail_donationchannel = '1'
               then min(cast(((datediff(mon, cast(detail_donationdate as date), dateadd(day, 5, current_date)))) as int))
           else 0 end comb_ltd_months_since_last_donation_mail,
       case
           when detail_donationchannel = '2'
               then min(cast(((datediff(mon, cast(detail_donationdate as date), dateadd(day, 5, current_date)))) as int))
           else 0 end comb_ltd_months_since_last_donation_phone,
       case
           when detail_donationchannel = '3'
               then min(cast(((datediff(mon, cast(detail_donationdate as date), dateadd(day, 5, current_date)))) as int))
           else 0 end comb_ltd_months_since_last_donation_web,
       case
           when detail_donationchannel = '4'
               then min(cast(((datediff(mon, cast(detail_donationdate as date), dateadd(day, 5, current_date)))) as int))
           else 0 end comb_ltd_months_since_last_donation_mobile
from ctas_trans_comb_tobedropped
where detail_donationchannel in ('1', '2', '3', '4')
group by company_id, detail_donationchannel;


drop table if exists ctas_response_channel_tobedropped;
create table ctas_response_channel_tobedropped
as
select hh.company_id,
       max(det.detail_donationchannel) as comb_ltd_last_response_channel
  from ctas_donation_tobedropped hh
  inner join ctas_trans_comb_tobedropped det
       on hh.company_id = det.company_id
       and hh.comb_ltd_last_donation_date = det.detail_donationdate
group by hh.company_id;


drop table if exists ctas_m06_donation_tobedropped;
create table ctas_m06_donation_tobedropped
as
select max(detail_donationdollar)     comb_m06_highest_donation_amount,
       min(detail_donationdollar)     comb_m06_lowest_donation_amount,
       count(*)                       comb_m06_total_number_of_donations,
       sum(nps.detail_donationdollar) comb_m06_total_dollar_donations,
       avg(nps.detail_donationdollar) comb_m06_average_donation_per_transaction,
       company_id
from ctas_trans_comb_tobedropped nps
where datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) <= 06
group by company_id;


drop table if exists ctas_m12_donation_tobedropped;
create table ctas_m12_donation_tobedropped
as
select max(detail_donationdollar)     comb_m12_highest_donation_amount,
       min(detail_donationdollar)     comb_m12_lowest_donation_amount,
       count(*)                       comb_m12_total_number_of_donations,
       sum(nps.detail_donationdollar) comb_m12_total_dollar_donations,
       avg(nps.detail_donationdollar) comb_m12_average_donation_per_transaction,
       company_id
from ctas_trans_comb_tobedropped nps
where datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) <= 12
group by company_id;


drop table if exists ctas_m24_donation_tobedropped;
create table ctas_m24_donation_tobedropped
as
select max(detail_donationdollar)     comb_m24_highest_donation_amount,
       min(detail_donationdollar)     comb_m24_lowest_donation_amount,
       count(*)                       comb_m24_total_number_of_donations,
       sum(nps.detail_donationdollar) comb_m24_total_dollar_donations,
       avg(nps.detail_donationdollar) comb_m24_average_donation_per_transaction,
       company_id
from ctas_trans_comb_tobedropped nps
where datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) <= 24
group by company_id;


drop table if exists ctas_m48_donation_tobedropped;
create table ctas_m48_donation_tobedropped
as
select max(detail_donationdollar)     comb_m48_highest_donation_amount,
       min(detail_donationdollar)     comb_m48_lowest_donation_amount,
       count(*)                       comb_m48_total_number_of_donations,
       sum(nps.detail_donationdollar) comb_m48_total_dollar_donations,
       avg(nps.detail_donationdollar) comb_m48_average_donation_per_transaction,
       company_id
from ctas_trans_comb_tobedropped nps
where datediff(mon, cast(nps.detail_donationdate as date), dateadd(day, 5, current_date)) <= 48
group by company_id;

-- 264 Starts
/*
 Create Apogee Child6 Table Step 3 of 4 - FEC Summary Combined with Child1 + Child5.

 Update Schedule: Monthly
*/

drop table if exists ctas_sum_comb_np_fec_tobedropped;
create table ctas_sum_comb_np_fec_tobedropped
(
    id                                              bigint encode az64,
    company_id                                      varchar(25),
    comb_ltd_first_donation_date                    char(8) encode zstd,
    comb_ltd_months_since_first_donation_date       bigint encode az64,
    comb_ltd_months_since_last_donation_date        bigint encode az64,
    comb_ltd_last_donation_date                     char(8) encode zstd,
    comb_ltd_number_of_donations_0_3_months_ago     bigint encode az64,
    comb_ltd_number_of_donations_0_6_months_ago     bigint encode az64,
    comb_ltd_number_of_donations_0_12_months_ago    bigint encode az64,
    comb_ltd_number_of_donations_0_24_months_ago    bigint encode az64,
    comb_ltd_number_of_donations_0_36_months_ago    bigint encode az64,
    comb_ltd_number_of_donations_0_48_months_ago    bigint encode az64,
    comb_ltd_number_of_donations_0_96_months_ago    bigint encode az64,
    comb_ltd_number_of_donations_over_96_months_ago bigint encode az64,
    comb_ltd_last_response_channel                  char(1) encode zstd,
    comb_ltd_months_since_last_donation_mail        bigint encode az64,
    comb_ltd_months_since_last_donation_phone       bigint encode az64,
    comb_ltd_months_since_last_donation_web         bigint encode az64,
    comb_ltd_months_since_last_donation_mobile      bigint encode az64,
    comb_ltd_highest_donation_amount_ever           bigint encode az64,
    comb_ltd_lowest_donation_amount_ever            bigint encode az64,
    comb_ltd_total_number_of_donations              bigint encode az64,
    comb_ltd_total_dollar_donations                 bigint encode az64,
    comb_ltd_average_donation_per_transaction       bigint encode az64,
    comb_m06_highest_donation_amount                bigint encode az64,
    comb_m06_lowest_donation_amount                 bigint encode az64,
    comb_m06_total_number_of_donations              bigint encode az64,
    comb_m06_total_dollar_donations                 bigint encode az64,
    comb_m06_average_donation_per_transaction       bigint encode az64,
    comb_m12_highest_donation_amount                bigint encode az64,
    comb_m12_lowest_donation_amount                 bigint encode az64,
    comb_m12_total_number_of_donations              bigint encode az64,
    comb_m12_total_dollar_donations                 bigint encode az64,
    comb_m12_average_donation_per_transaction       bigint encode az64,
    comb_m24_highest_donation_amount                bigint encode az64,
    comb_m24_lowest_donation_amount                 bigint encode az64,
    comb_m24_total_number_of_donations              bigint encode az64,
    comb_m24_total_dollar_donations                 bigint encode az64,
    comb_m24_average_donation_per_transaction       bigint encode az64,
    comb_m48_highest_donation_amount                bigint encode az64,
    comb_m48_lowest_donation_amount                 bigint encode az64,
    comb_m48_total_number_of_donations              bigint encode az64,
    comb_m48_total_dollar_donations                 bigint encode az64,
    comb_m48_average_donation_per_transaction       bigint encode az64
)
    diststyle key
    distkey (company_id)
    sortkey (company_id)
;

insert into ctas_sum_comb_np_fec_tobedropped
    (select a.id                                                 as id,
            a.company_id                                         as company_id,
            b.comb_ltd_first_donation_date                       as comb_ltd_first_donation_date,
            datediff(month, cast(b.comb_ltd_first_donation_date as date), dateadd(day, 5, current_date))  as comb_ltd_months_since_first_donation_date,
            datediff(month, cast(b.comb_ltd_last_donation_date  as date), dateadd(day, 5, current_date))  as comb_ltd_months_since_last_donation_date,
            b.comb_ltd_last_donation_date                        as comb_ltd_last_donation_date,
            b.comb_ltd_number_of_donations_0_3_months_ago        as comb_ltd_number_of_donations_0_3_months_ago,
            b.comb_ltd_number_of_donations_0_6_months_ago        as comb_ltd_number_of_donations_0_6_months_ago,
            b.comb_ltd_number_of_donations_0_12_months_ago       as comb_ltd_number_of_donations_0_12_months_ago,
            b.comb_ltd_number_of_donations_0_24_months_ago       as comb_ltd_number_of_donations_0_24_months_ago,
            b.comb_ltd_number_of_donations_0_36_months_ago       as comb_ltd_number_of_donations_0_36_months_ago,
            b.comb_ltd_number_of_donations_0_48_months_ago       as comb_ltd_number_of_donations_0_48_months_ago,
            b.comb_ltd_number_of_donations_0_96_months_ago       as comb_ltd_number_of_donations_0_96_months_ago,
            b.comb_ltd_number_of_donations_over_96_months_ago    as comb_ltd_number_of_donations_over_96_months_ago,
            d.comb_ltd_last_response_channel                     as comb_ltd_last_response_channel,
            c1.comb_ltd_months_since_last_donation_mail          as comb_ltd_months_since_last_donation_mail,
            c2.comb_ltd_months_since_last_donation_phone         as comb_ltd_months_since_last_donation_phone,
            c3.comb_ltd_months_since_last_donation_web           as comb_ltd_months_since_last_donation_web,
            c4.comb_ltd_months_since_last_donation_mobile        as comb_ltd_months_since_last_donation_mobile,
            b.comb_ltd_highest_donation_amount_ever              as comb_ltd_highest_donation_amount_ever,
            b.comb_ltd_lowest_donation_amount_ever               as comb_ltd_lowest_donation_amount_ever,
            b.comb_ltd_total_number_of_donations                 as comb_ltd_total_number_of_donations,
            b.comb_ltd_total_dollar_donations                    as comb_ltd_total_dollar_donations,
            b.comb_ltd_average_donation_per_transaction          as comb_ltd_average_donation_per_transaction,
            e.comb_m06_highest_donation_amount                   as comb_m06_highest_donation_amount,
            e.comb_m06_lowest_donation_amount                    as comb_m06_lowest_donation_amount,
            e.comb_m06_total_number_of_donations                 as comb_m06_total_number_of_donations,
            e.comb_m06_total_dollar_donations                    as comb_m06_total_dollar_donations,
            e.comb_m06_average_donation_per_transaction          as comb_m06_average_donation_per_transaction,
            f.comb_m12_highest_donation_amount                   as comb_m12_highest_donation_amount,
            f.comb_m12_lowest_donation_amount                    as comb_m12_lowest_donation_amount,
            f.comb_m12_total_number_of_donations                 as comb_m12_total_number_of_donations,
            f.comb_m12_total_dollar_donations                    as comb_m12_total_dollar_donations,
            f.comb_m12_average_donation_per_transaction          as comb_m12_average_donation_per_transaction,
            g.comb_m24_highest_donation_amount                   as comb_m24_highest_donation_amount,
            g.comb_m24_lowest_donation_amount                    as comb_m24_lowest_donation_amount,
            g.comb_m24_total_number_of_donations                 as comb_m24_total_number_of_donations,
            g.comb_m24_total_dollar_donations                    as comb_m24_total_dollar_donations,
            g.comb_m24_average_donation_per_transaction          as comb_m24_average_donation_per_transaction,
            h.comb_m48_highest_donation_amount                   as comb_m48_highest_donation_amount,
            h.comb_m48_lowest_donation_amount                    as comb_m48_lowest_donation_amount,
            h.comb_m48_total_number_of_donations                 as comb_m48_total_number_of_donations,
            h.comb_m48_total_dollar_donations                    as comb_m48_total_dollar_donations,
            h.comb_m48_average_donation_per_transaction          as comb_m48_average_donation_per_transaction
     from ctas_companyid_tobedropped a
              left join ctas_donation_tobedropped b
                        on a.company_id = b.company_id
              left join ctas_channelsummary_tobedropped c1
                        on a.company_id = c1.company_id and c1.detail_donationchannel = '1'
              left join ctas_channelsummary_tobedropped c2
                        on a.company_id = c2.company_id and c2.detail_donationchannel = '2'
              left join ctas_channelsummary_tobedropped c3
                        on a.company_id = c3.company_id and c3.detail_donationchannel = '3'
              left join ctas_channelsummary_tobedropped c4
                        on a.company_id = c4.company_id and c4.detail_donationchannel = '4'
              left join ctas_response_channel_tobedropped d
                        on b.company_id = d.company_id
              left join ctas_m06_donation_tobedropped e
                        on a.company_id = e.company_id
              left join ctas_m12_donation_tobedropped f
                        on a.company_id = f.company_id
              left join ctas_m24_donation_tobedropped g
                        on a.company_id = g.company_id
              left join ctas_m48_donation_tobedropped h
                        on a.company_id = h.company_id)
;

--drop temp tables
--drop table if exists ctas_trans_comb_tobedropped;  --exclude_trans_np and exclude_trans_fec
--drop table if exists ctas_companyid_tobedropped;    -- unique company_id from trans
drop table if exists ctas_channelsummary_tobedropped; -- ltd mos since last donation + channel
drop table if exists ctas_donation_tobedropped;     -- combined ltd calcs
drop table if exists ctas_response_channel_tobedropped;
drop table if exists ctas_m06_donation_tobedropped;
drop table if exists ctas_m12_donation_tobedropped;
drop table if exists ctas_m24_donation_tobedropped;
drop table if exists ctas_m48_donation_tobedropped;

-- 266 Starts

drop table if exists exclude_nonprofit_tblchild6_comb_{dbid};
create table exclude_nonprofit_tblchild6_comb_{dbid}
(
    company_id                                      varchar(25)   ,
    comb_ltd_first_donation_date                    char(8)       encode zstd,
    comb_ltd_months_since_first_donation_date       bigint       encode az64,
    comb_ltd_months_since_last_donation_date        bigint       encode az64,
    comb_ltd_last_donation_date                     char(8)       encode zstd,
    comb_ltd_number_of_donations_0_3_months_ago     bigint       encode az64,
    comb_ltd_number_of_donations_0_6_months_ago     bigint       encode az64,
    comb_ltd_number_of_donations_0_12_months_ago    bigint       encode az64,
    comb_ltd_number_of_donations_0_24_months_ago    bigint       encode az64,
    comb_ltd_number_of_donations_0_36_months_ago    bigint       encode az64,
    comb_ltd_number_of_donations_0_48_months_ago    bigint       encode az64,
    comb_ltd_number_of_donations_0_96_months_ago    bigint       encode az64,
    comb_ltd_number_of_donations_over_96_months_ago bigint       encode az64,
    comb_ltd_last_response_channel                  char(1)       encode zstd,
    comb_ltd_months_since_last_donation_mail        bigint       encode az64,
    comb_ltd_months_since_last_donation_phone       bigint       encode az64,
    comb_ltd_months_since_last_donation_web         bigint       encode az64,
    comb_ltd_months_since_last_donation_mobile      bigint       encode az64,
    comb_ltd_highest_donation_amount_ever           bigint       encode az64,
    comb_ltd_lowest_donation_amount_ever            bigint       encode az64,
    comb_ltd_total_number_of_donations              bigint       encode az64,
    comb_ltd_total_dollar_donations                 bigint       encode az64,
    comb_ltd_average_donation_per_transaction       bigint       encode az64,
    comb_m06_highest_donation_amount                bigint       encode az64,
    comb_m06_lowest_donation_amount                 bigint       encode az64,
    comb_m06_total_number_of_donations              bigint       encode az64,
    comb_m06_total_dollar_donations                 bigint       encode az64,
    comb_m06_average_donation_per_transaction       bigint       encode az64,
    comb_m12_highest_donation_amount                bigint       encode az64,
    comb_m12_lowest_donation_amount                 bigint       encode az64,
    comb_m12_total_number_of_donations              bigint       encode az64,
    comb_m12_total_dollar_donations                 bigint       encode az64,
    comb_m12_average_donation_per_transaction       bigint       encode az64,
    comb_m24_highest_donation_amount                bigint       encode az64,
    comb_m24_lowest_donation_amount                 bigint       encode az64,
    comb_m24_total_number_of_donations              bigint       encode az64,
    comb_m24_total_dollar_donations                 bigint       encode az64,
    comb_m24_average_donation_per_transaction       bigint       encode az64,
    comb_m48_highest_donation_amount                bigint       encode az64,
    comb_m48_lowest_donation_amount                 bigint       encode az64,
    comb_m48_total_number_of_donations              bigint       encode az64,
    comb_m48_total_dollar_donations                 bigint       encode az64,
    comb_m48_average_donation_per_transaction       bigint       encode az64
)
    diststyle key
    distkey (company_id)
    sortkey (company_id)
;

insert into exclude_nonprofit_tblchild6_comb_{dbid}
    (select company_id
          , comb_ltd_first_donation_date
          , comb_ltd_months_since_first_donation_date
          , comb_ltd_months_since_last_donation_date
          , comb_ltd_last_donation_date
          , comb_ltd_number_of_donations_0_3_months_ago
          , comb_ltd_number_of_donations_0_6_months_ago
          , comb_ltd_number_of_donations_0_12_months_ago
          , comb_ltd_number_of_donations_0_24_months_ago
          , comb_ltd_number_of_donations_0_36_months_ago
          , comb_ltd_number_of_donations_0_48_months_ago
          , comb_ltd_number_of_donations_0_96_months_ago
          , comb_ltd_number_of_donations_over_96_months_ago
          , comb_ltd_last_response_channel
          , comb_ltd_months_since_last_donation_mail
          , comb_ltd_months_since_last_donation_phone
          , comb_ltd_months_since_last_donation_web
          , comb_ltd_months_since_last_donation_mobile
          , comb_ltd_highest_donation_amount_ever
          , comb_ltd_lowest_donation_amount_ever
          , comb_ltd_total_number_of_donations
          , comb_ltd_total_dollar_donations
          , comb_ltd_average_donation_per_transaction
          , comb_m06_highest_donation_amount
          , comb_m06_lowest_donation_amount
          , comb_m06_total_number_of_donations
          , comb_m06_total_dollar_donations
          , comb_m06_average_donation_per_transaction
          , comb_m12_highest_donation_amount
          , comb_m12_lowest_donation_amount
          , comb_m12_total_number_of_donations
          , comb_m12_total_dollar_donations
          , comb_m12_average_donation_per_transaction
          , comb_m24_highest_donation_amount
          , comb_m24_lowest_donation_amount
          , comb_m24_total_number_of_donations
          , comb_m24_total_dollar_donations
          , comb_m24_average_donation_per_transaction
          , comb_m48_highest_donation_amount
          , comb_m48_lowest_donation_amount
          , comb_m48_total_number_of_donations
          , comb_m48_total_dollar_donations
          , comb_m48_average_donation_per_transaction
     from ctas_sum_comb_np_fec_tobedropped);

drop table if exists ctas_sum_comb_np_fec_tobedropped;

*/