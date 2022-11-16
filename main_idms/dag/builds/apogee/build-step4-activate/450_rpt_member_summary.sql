/*
For testing only Use common validate report.

DO NOT DELETE - KEEP FOR REFERENCE
*/

drop table if exists no_such_table; --exclude_build_list_summary_apogee;

/*
drop table if exists exclude_build_list_summary_apogee;
create table exclude_build_list_summary_apogee
(
    databaseid            INTEGER       ENCODE az64,
    buildid               INTEGER       ENCODE az64,
    dcreateddate          TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    listid                INTEGER       ENCODE az64,
    dmintransdate         DATE          ENCODE az64,
    dmaxtransdate         DATE          ENCODE az64,
    transactions          BIGINT        ENCODE az64,
    avgtransdollar        INTEGER       ENCODE az64,
    householddonors       BIGINT        ENCODE az64,
    individualdonors      BIGINT        ENCODE az64,
    accountdonors         BIGINT        ENCODE az64,
    donormatchcons_hh     BIGINT        ENCODE az64,
    donormatchcons_ind    BIGINT        ENCODE az64,
    listcategory01        VARCHAR(5)    ENCODE lzo,
    listcategory02        VARCHAR(5)    ENCODE lzo,
    listcategory03        VARCHAR(5)    ENCODE lzo,
    listcategory04        VARCHAR(5)    ENCODE lzo
)
    diststyle key
    distkey (buildid)
    sortkey (buildid)
;

grant select, update, insert, delete, trigger, references on exclude_build_list_summary_apogee to airflow_app;
grant select, update, insert, delete, trigger, references on exclude_build_list_summary_apogee to group da_developer_grp;


-- gets listnames
drop table if exists #listnames;
select id,
       replace(replace(clistname,',',' '),'|',' ') listname
  into #listnames
  from sql_tblmasterlol
 where databaseid = {dbid};


--get unique hh ind DONORS that match to Consumer by list
drop table if exists exclude_apg_trans_list_hh;
create table exclude_apg_trans_list_hh
    (sourcelistid varchar(10), company_id varchar(25));
insert into exclude_apg_trans_list_hh (sourcelistid, company_id)
    select sourcelistid, company_id from exclude_trans_np
     group by sourcelistid, company_id
    union
    select sourcelistid, company_id from exclude_trans_fec
     group by sourcelistid, company_id;

drop table if exists exclude_apg_trans_list_ind;
create table exclude_apg_trans_list_ind
    (sourcelistid varchar(10), individual_id varchar(25));
insert into exclude_apg_trans_list_ind (sourcelistid, individual_id)
    select sourcelistid, individual_id from exclude_trans_np
     group by sourcelistid, individual_id
    union
    select sourcelistid, individual_id from exclude_trans_fec
     group by sourcelistid, individual_id;

-- find consumer matches by list and combine
drop table if exists listmatch_tobedropped;
select 'household' as matchlevel,
       sourcelistid,
       cast(SUM(CASE WHEN REGEXP_COUNT(company_id, '^[0-9]+$') != 0 THEN 1 ELSE 0 END) as bigint) as hh_match,
       cast(0 as bigint) as ind_match
into listmatch_tobedropped
from exclude_apg_trans_list_hh group by sourcelistid
union
select 'individual' as matchlevel,
       sourcelistid,
       cast(0 as bigint) as hh_match,
       cast(SUM(CASE WHEN REGEXP_COUNT(individual_id, '^[0-9]+$') != 0 THEN 1 ELSE 0 END) as bigint) as ind_match
from exclude_apg_trans_list_ind group by sourcelistid
order by sourcelistid, matchlevel;

-- create report
drop table if exists ctas_apogee_final_counts_{build_id}_tobedropped;
create table ctas_apogee_final_counts_{build_id}_tobedropped as
    (
        select
            {dbid}                          as databaseid,
            {build_id}                      as buildid,
            cast(current_date  as timestamp without time zone) as dcreateddate,
            c3.sourcelistid                 as listid,
            listname                        as listname,
            min(detail_donationdate)        as dmintransdate,
            max(detail_donationdate)        as dmaxtransdate,
            count(*)                        as transactions,
            avg(cast(detail_donationdollar  as bigint)) as avgtransdollar,
            count(distinct company_id)      as householddonors,
            count(distinct individual_id)   as individualdonors,
            count(distinct accountno)       as accountdonors,
            max(hh_match)                   as donormatchcons_hh,
            max(ind_match)                  as donormatchcons_ind,
            listcategory01                  as listcategory01,
            listcategory02                  as listcategory02,
            listcategory03                  as listcategory03,
            listcategory04                  as listcategory04
        from  tblchild3_{build_id}_{build} c3
        inner join #listnames lnames                   on c3.sourcelistid = lnames.id
        inner join listmatch_tobedropped lmatch        on c3.sourcelistid = lmatch.sourcelistid
        group by c3.sourcelistid, listname, listcategory01, listcategory02, listcategory03, listcategory04
        union
        select
            {dbid}                          as databaseid,
            {build_id}                      as buildid,
            cast(current_date  as TIMESTAMP WITHOUT TIME ZONE) as dcreateddate,
            c4.sourcelistid                 as listid,
            listname                        as listname,
            min(detail_donationdate)        as dmintransdate,
            max(detail_donationdate)        as dmaxtransdate,
            count(*)                        as transactions,
            avg(cast(detail_donationdollar  as bigint)) as avgtransdollar,
            count(distinct company_id)      as householddonors,
            count(distinct individual_id)   as individualdonors,
            count(distinct accountno)       as accountdonors,
            max(hh_match)                   as donormatchcons_hh,
            max(ind_match)                  as donormatchcons_ind,
            listcategory01                  as listcategory01,
            listcategory02                  as listcategory02,
            listcategory03                  as listcategory03,
            listcategory04                  as listcategory04
        from  tblchild4_{build_id}_{build} c4
        inner join #listnames lnames                   on c4.sourcelistid = lnames.id
        inner join listmatch_tobedropped lmatch        on c4.sourcelistid = lmatch.sourcelistid
        group by c4.sourcelistid, listname, listcategory01, listcategory02, listcategory03, listcategory04
        order by listid
    );


unload ('select buildid             as "Build ID",
                dcreateddate        as "Build Date",
                listid              as "List ID",
                listname            as "List Member Name",
                dmintransdate       as "From Date",
                dmaxtransdate       as "Thru Date",
                transactions        as "Total Records",
                avgtransdollar      as "Avg Donation",
                householddonors     as "Household Unique Donors",
                individualdonors    as "Individual Unique Donors",
                accountdonors       as "Account Unique Donors",
                donormatchcons_hh   as "Donor Matches Consumer - Household",
                donormatchcons_ind  as "Donor Matches Consumer - Individual",
                listcategory01      as "Primary Category",
                listcategory02      as "Secondary Category",
                listcategory03      as "Tertiary Category",
                listcategory04      as "Primary Category - FEC"
           from ctas_apogee_final_counts_{build_id}_tobedropped')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off;


delete from exclude_build_list_summary_apogee
      where buildid = {build_id};

insert into exclude_build_list_summary_apogee
    (databaseid, buildid, dcreateddate, listid, dmintransdate, dmaxtransdate, transactions, avgtransdollar,
            householddonors, individualdonors, accountdonors, donormatchcons_hh, donormatchcons_ind,
            listcategory01, listcategory02, listcategory03, listcategory04)
    (select databaseid, buildid, dcreateddate, cast(listid as int) as listid,
            cast(dmintransdate as DATE), cast(dmaxtransdate as DATE), transactions, avgtransdollar,
            householddonors, individualdonors, accountdonors, donormatchcons_hh, donormatchcons_ind,
            listcategory01, listcategory02, listcategory03, listcategory04
       from ctas_apogee_final_counts_{build_id}_tobedropped order by buildid, listname);

*/
