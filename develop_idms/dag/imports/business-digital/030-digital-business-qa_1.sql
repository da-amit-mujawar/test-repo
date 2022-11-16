drop table if exists digital.my_business_fullset;

create table digital.my_business_fullset as select * from digital.business_main_qa  where state = 'RI' ;
---(182242 row(s) affected)


select state,count(*) from digital.business_main_qa group by state order by state;

drop table if exists digital.business_exec_fullset;

create table digital.business_exec_fullset as
select e.* from digital.business_exec_fullset_qa e inner join digital.my_business_fullset b on ( e.abinumber = b.abinumber);
---(836550 row(s) affected)

select * from digital.business_exec_fullset limit 100;

drop table if exists digital.business_email_fullset;

create table digital.business_email_fullset as
select e.*
from digital.business_email_fullset_qa e
where ltrim(rtrim(firstname)) != ''
and (abinumber = '000000000' or ltrim(rtrim(abinumber)) = '')
and state = 'RI' ;
---(264736 row(s) affected)

drop table if exists digital.business_sic_fullset;

create table digital.business_sic_fullset as
select e.*
from digital.business_sic_fullset_qa  e inner join digital.my_business_fullset b on ( e.abinumber = b.abinumber) ;
---(239760 row(s) affected)


drop table if exists digital.consumer_indiv;

create table digital.consumer_indiv as
select e_individualid
from digital.business_exec_fullset
where e_individualid != '000000000000'
group by e_individualid;
---(123633 row(s) affected)


drop table if exists digital.consumer_email_fullset;

create table digital.consumer_email_fullset as
select e.*
from digital.consumer_email_fullset_qa e join digital.consumer_indiv b on ( e.individualid = b.e_individualid);
---(175779 row(s)

select count(*) from digital.consumer_email_fullset;
---820799

drop table if exists digital.consumer_family;

create table digital.consumer_family as
select e_familyid
from digital.business_exec_fullset
where e_familyid != '000000000000'
and ( e_individualid != '' and  e_individualid != '000000000000')
group by e_familyid;
---(114930 row(s) affected)

select count(*) from digital.consumer_family;
---471850
select count(*) from digital.consumer_email_fullset;
---820799

insert into digital.consumer_email_fullset (
id, acquisition_date, vendorid2, usage_indicator, firstname, d_firstname, middleinitial, lastname, d_lastname, gender, housenumber, streetpredirectional, streetname, d_streetname, streetsuffix, streetpostdirectional, unittype, unitnumber, city, d_city, state, zipcode, zipfour, move_ind, emailaddress, responderdate, suppression_type, familyid, individualid, match_ind, mailconfidence, recordtype, alsincome, alsagecode, alslengthofresidence, alspurchasingpowerincome, alshomevalue, alswealthfinder, psfducode, p10ducode, pownrocccode, ip_address, optin_date, url, statecode, censuscountycode, censustract, censusblockgroup, matchcode, editedaddress, emaildatabase_extent, oldmasterindicator, prioritysourcecode, roadrunner_flag, source_counter, source_code_indicator1, source_code_indicator2, source_code_indicator3, latitude, longitude, vendorcustomerid, gst_sourcecode_indicator, gst_source_counter, lemsmatchcode, reject_reason, dma_code, bvt_email_status, bvt_refresh_date, ipst_validity_score, ipst_status_code, ipst_refresh_date, category, email_clickthru_date, email_open_date, domain, top_level_domain, dob_individual, home_owner, best_date, emaildb_flag, countrycode
)
select e.* from digital.consumer_email_fullset_qa e inner join digital.consumer_family b
on ( e.familyid = b.e_familyid);
---(449581 row(s) affected)

select count(*) from digital.consumer_email_fullset;
---2845828

drop table if exists digital.business_occupations;

create table digital.business_occupations as
select *
from digital.business_occupations_qa where state = 'RI';
---(148482 row(s) affected)

drop table if exists digital.consumer_indiv;

create table digital.consumer_indiv as
select individual_id
from digital.business_occupations
where (individual_id != '000000000000' and individual_id !='')
group by individual_id;
---(31565 row(s) affected)

insert into digital.consumer_email_fullset
select e.* from digital.consumer_email_fullset_qa e join digital.consumer_indiv b on ( e.individualid = b.individual_id);
---(48659 row(s) affected)

select count(*) from digital.consumer_email_fullset;
---3097408

drop table if exists digital.consumer_family;

create table digital.consumer_family as
select family_id
from digital.business_occupations
where (family_id != '000000000000' and family_id !='')
and ( individual_id   != '' and  individual_id != '000000000000')
group by family_id;
---(30285 row(s) affected)

insert into digital.consumer_email_fullset
select e.* from digital.consumer_email_fullset_qa e join digital.consumer_family b
on ( e.familyid = b.family_id);
---(128368 row(s) affected)

---Stop!!!!











