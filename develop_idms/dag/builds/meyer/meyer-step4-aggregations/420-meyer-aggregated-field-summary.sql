/*
Aggregated Field Summary for auditing
*/

/* Summary by Listid and field */
drop table if exists meyer_aggregations_summary_listid;
    create table meyer_aggregations_summary_listid (
        listid varchar(10),
        field_description varchar(50),
        field_value varchar(50),
        record_count int);

insert into meyer_aggregations_summary_listid
    (listid, field_description, field_value, record_count)
select listid, 'MYR SCHOOL ID', myrschoolid, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR FINAL ALUMNI GENDER', myrfinalalumnigender, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR FINAL SPOUSE GENDER', myrfinalspousegender, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR ALUMNI CALCULATED AGE', myralumnicalculatedage, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR ALUMNI INFERRED AGE', myralumniinferredage, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR ROUNDED AGE', myrroundedage, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR SEGMENTATION AGE', myrsegmentationage, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR SPOUSE MATCHED AGE', myrspousematchedage, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR ALUMNI AGE RANGE', myralumniagerange, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR ALUMNI GENERATIONAL SUFFIX', myralumnigenerationalsuffix, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR SPOUSE GENERATIONAL SUFFIX', myrspousegenerationalsuffix, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR FINAL ALUMNI MARRIED', myrfinalalumnimarried, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR GENERIC SALUTATION', myrgenericsalutation, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR INFORCE FLAG', myrinforceflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR OPT OUT FLAG', myroptoutflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR LTC FLAG', myrltcflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR TRAVEL FLAG', myrtravelflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR AUTO MET FLAG', myrautometflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR AUTO LIBERTY FLAG', myrautolibertyflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR AUTO BINDABLE FLAG', myrautobindableflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR ADVISORY FLAG', myradvisoryflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR REAL ESTATE FLAG', myrrealestateflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR  LAUREL ROAD FLAG', myrlaurelroadflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR ID PROTECTION FLAG', myridprotectionflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR  PET FLAG', myrpetflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MYR  PRUDENTIAL FLAG', myrprudentialflag, count(*) from {maintable_name}  where listid <> 18702 group by 1,3 union
        select listid, 'MAILABLE', mailable, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR SCHOOL PRIORITY', myrschoolpriority, count(*) from {maintable_name} where listid <> 18702 group by 1,3 union
        select listid, 'MYR SOURCE TABLE', myrsourcetable, count(*) from {maintable_name} group by 1,3
        order by 1,2,3;

unload ('select * from meyer_aggregations_summary_listid order by listid, field_description, field_value;')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
header
delimiter as '|'
parallel off
allowoverwrite
gzip
;


/* Summary for Database by field */
drop table if exists meyer_aggregations_summary;
    create table meyer_aggregations_summary (
        field_description varchar(50),
        field_value varchar(50),
        record_count int);

insert into meyer_aggregations_summary
    (field_description, field_value, record_count)
select 'MYR SCHOOL ID', myrschoolid, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR FINAL ALUMNI GENDER', myrfinalalumnigender, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR FINAL SPOUSE GENDER', myrfinalspousegender, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR ALUMNI CALCULATED AGE', myralumnicalculatedage, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR ALUMNI INFERRED AGE', myralumniinferredage, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR ROUNDED AGE', myrroundedage, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR SEGMENTATION AGE', myrsegmentationage, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR SPOUSE MATCHED AGE', myrspousematchedage, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR ALUMNI AGE RANGE', myralumniagerange, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR ALUMNI GENERATIONAL SUFFIX', myralumnigenerationalsuffix, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR SPOUSE GENERATIONAL SUFFIX', myrspousegenerationalsuffix, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR FINAL ALUMNI MARRIED', myrfinalalumnimarried, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR GENERIC SALUTATION', myrgenericsalutation, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR INFORCE FLAG', myrinforceflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR OPT OUT FLAG', myroptoutflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR LTC FLAG', myrltcflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR TRAVEL FLAG', myrtravelflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR AUTO MET FLAG', myrautometflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR AUTO LIBERTY FLAG', myrautolibertyflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR AUTO BINDABLE FLAG', myrautobindableflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR ADVISORY FLAG', myradvisoryflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR REAL ESTATE FLAG', myrrealestateflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR  LAUREL ROAD FLAG', myrlaurelroadflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR ID PROTECTION FLAG', myridprotectionflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR  PET FLAG', myrpetflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MYR  PRUDENTIAL FLAG', myrprudentialflag, count(*) from {maintable_name}  where listid <> 18702 group by 2 union
        select 'MAILABLE', mailable, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR SCHOOL PRIORITY', myrschoolpriority, count(*) from {maintable_name} where listid <> 18702 group by 2 union
        select 'MYR SOURCE TABLE', myrsourcetable, count(*) from {maintable_name} group by 2
        order by 1,2;

unload ('select * from meyer_aggregations_summary order by field_description, field_value;')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
header
delimiter as '|'
parallel off
allowoverwrite
gzip
;

/* Summary for Product Codes by state, age_range */
drop table if exists meyer_aggregations_product_summary;
    create table meyer_aggregations_product_summary (
        product_name varchar(100),
        ahi_state varchar(100),
        myr_alumni_age_range varchar(100),
        product_code varchar(100),
        record_count int);


insert into meyer_aggregations_product_summary
    (product_name, ahi_state, myr_alumni_age_range, product_code, record_count)
select 'MYR_PRODUCT_CORE', state, myralumniagerange, myr_product_core, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_CORE_QD', state, myralumniagerange, myr_product_core_qd, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_L4L_ART_10YR', state, myralumniagerange, myr_product_l4l_art_10yr, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_L4L_ART_20YR', state, myralumniagerange, myr_product_l4l_art_20yr, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_L4L_SAP_10YR', state, myralumniagerange, myr_product_l4l_sap_10yr, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_L4L_SAP_20YR', state, myralumniagerange, myr_product_l4l_sap_20yr, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_L95', state, myralumniagerange, myr_product_l95, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_AD_NYL', state, myralumniagerange, myr_product_ad_nyl, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_AD_MET', state, myralumniagerange, myr_product_ad_met, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_PRU_CI', state, myralumniagerange, myr_product_pru_ci, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_PRU_AC', state, myralumniagerange, myr_product_pru_ac, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_PRU_HIP', state, myralumniagerange, myr_product_pru_hip, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_LM_BAD_STATES', state, myralumniagerange, myr_product_lm_bad_states, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_LM_GOOD_STATES', state, myralumniagerange, myr_product_lm_good_states, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_LTC', state, myralumniagerange, myr_product_ltc, count(*) from {maintable_name} group by 1,2,3,4 union 
select 'MYR_PRODUCT_LTD', state, myralumniagerange, myr_product_ltd, count(*) from {maintable_name} group by 1,2,3,4 
order by 1,2,3,4;

unload ('select * from meyer_aggregations_product_summary order by product_name, ahi_state, myr_alumni_age_range, product_code;')
to 's3://{s3-internal}{reportname3}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
header
delimiter as '|'
parallel off
allowoverwrite
gzip
;
