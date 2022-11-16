/* 3.6	House List and Opt Out File
The Meyer’s Inforce records will be present on the database with their SOURCE TABLE field starting with ‘INFO’.
    If the Meyer’s Inforce file matches to alumni or other name and address records,
    all matching records will have a MYR INFORCE FLAG set  to ‘Y’.  Default value will be ‘N’.
    This flag can change its value from month to month depending on the data provided
    and the matching rules that have been established.

The OPT OUT records will be present on the database with their SOURCE TABLE field starting with ‘OPTO’.
    If an OPT OUT record matches to alumni records or other name and address records,
    all matching records will have a MYR OPT OUT FLAG set to ‘Y’.  Default value for this flag is ‘N’.
    This flag can change its value from month to month depending on the data provided
    and the matching rules that have been established.

The LTC records will be present on the database with their SOURCE TABLE field starting with ‘LTC’.
    If a LTC record matches to alumni records or other name and address records,
    all matching records will have a MYR LTC FLAG set to ‘Y’.  Default value for this flag is ‘N’.
    This flag can change its value from month to month depending on the data provided
    and the matching rules that have been established.

The Travel records will be present on the database with their SOURCE TABLE field starting with ‘TRAV’.
    If a Travel record matches to alumni records or other name and address records,
    all matching records will have a MYR TRAVEL FLAG set to ‘Y’.  Default value for this flag is ‘N’.
    This flag can change its value from month to month depending on the data provided
    and the matching rules that have been established.
*/
--20210803 added ALL flags, per Kim P
-- 20210920 fix
-- update {maintable_name}
--    set myrinforceflag = case when upper(i.myrsourcetable) like 'INFO%' then 'Y' end,
--        myroptoutflag = case when upper(i.myrsourcetable) like 'OPT%' then 'Y' end,
--        myrltcflag = case when upper(i.myrsourcetable) like 'LTC%' then 'Y' end,
--        myrtravelflag = case when upper(i.myrsourcetable) like 'TRAV%' then 'Y' end,
--        myrautolibertyflag = case when upper(i.myrsourcetable) like 'AUTO-LIB%' then 'Y' end,
--        myrautometflag = case when upper(i.myrsourcetable) like 'AUTO-MET%' then 'Y' end,
--        myrautobindableflag = case when upper(i.myrsourcetable) like 'AUTO-BIND%' then 'Y' end,
--        myradvisoryflag = case when upper(i.myrsourcetable) like 'ADVIS%' then 'Y' end,
--        myrrealestateflag = case when upper(i.myrsourcetable) like 'REAL%' then 'Y' end,
--        myrlaurelroadflag = case when upper(i.myrsourcetable) like 'LAUREL%' then 'Y' end,
--        myridprotectionflag = case when upper(i.myrsourcetable) like 'ID PROT%' then 'Y' end,
--        myrpetflag = case when upper(i.myrsourcetable) like 'PET%' then 'Y' end,
--        myrprudentialflag = case when upper(i.myrsourcetable) like 'PRUD%' then 'Y' end
--   from {maintable_name} m inner join
--        meyer_18702_ids_tobedropped i on m.mcdindividualid = i.mcdindividualid
--  and nvl(m.mcdindividualid,'') <> '';


-- add new field for 2nd-level matching - cb 20220304
--alter table {maintable_name} add upr_concat_fnlnaddr1 varchar(200) default '';

-- changed Level 2 Match JIRA-IDMS-2464 2022.10

update {maintable_name}
   set myrinforceflag = 'N',
       myroptoutflag = 'N',
       myrltcflag = 'N',
       myrtravelflag = 'N',
       myrautolibertyflag = 'N',
       myrautometflag = 'N',
       myrautobindableflag = 'N',
       myradvisoryflag = 'N',
       myrrealestateflag = 'N',
       myrlaurelroadflag = 'N',
       myridprotectionflag = 'N',
       myrpetflag = 'N',
       myrprudentialflag = 'N',
       --upr_concat_fnlnaddr1 no longer used JIRA-IDMS-2464 2022.10
       upr_concat_fnlnaddr1 =
           upper(replace(concat(concat(trim(firstname),trim(lastname)),
           trim(addressline1)),' ',''));

-- create table of house OOs for matching
-- added oldmyrid JIRA-IDMS-2464 2022.10
drop table if exists meyer_18702_ids_tobedropped;
select mcdindividualid, mcdhouseholdid, upper(myrsourcetable) upr_myrsourcetable, upr_concat_fnlnaddr1, oldmyrid
  into meyer_18702_ids_tobedropped
  from {maintable_name}
 where listid = 18702;

-- Level 1 Match on mcdindividualid
update {maintable_name} set myrinforceflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'INFO%');

update {maintable_name} set myroptoutflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'OPT%');

update {maintable_name} set myrltcflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'LTC%');

update {maintable_name} set myrtravelflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'TRAV%');

update {maintable_name} set myrautolibertyflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-LIB%');

update {maintable_name} set myrautometflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-MET%');

update {maintable_name} set myrautobindableflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-BIND%');

update {maintable_name} set myradvisoryflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'ADVIS%');

update {maintable_name} set myrrealestateflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'REAL%');

update {maintable_name} set myrlaurelroadflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'LAUREL%');

update {maintable_name} set myridprotectionflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'ID PROT%');

update {maintable_name} set myrpetflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'PET%');

update {maintable_name} set myrprudentialflag = 'Y'
where mcdindividualid in (select mcdindividualid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'PRUD%');

-- changed Level 2 Match JIRA-IDMS-2464 2022.10
-- -- Level 2 Match on upr_concat_fnlnaddr1
-- update {maintable_name} set myrinforceflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'INFO%' and myrinforceflag = 'N');
--
-- update {maintable_name} set myroptoutflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'OPT%' and myroptoutflag = 'N');
--
-- update {maintable_name} set myrltcflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'LTC%' and myrltcflag = 'N');
--
-- update {maintable_name} set myrtravelflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'TRAV%' and myrtravelflag = 'N');
--
-- update {maintable_name} set myrautolibertyflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-LIB%' and myrautolibertyflag = 'N');
--
-- update {maintable_name} set myrautometflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-MET%' and myrautometflag = 'N');
--
-- update {maintable_name} set myrautobindableflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-BIND%' and myrautobindableflag = 'N');
--
-- update {maintable_name} set myradvisoryflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'ADVIS%' and myradvisoryflag = 'N');
--
-- update {maintable_name} set myrrealestateflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'REAL%' and myrrealestateflag = 'N');
--
-- update {maintable_name} set myrlaurelroadflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'LAUREL%' and myrlaurelroadflag = 'N');
--
-- update {maintable_name} set myridprotectionflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'ID PROT%' and myridprotectionflag = 'N');
--
-- update {maintable_name} set myrpetflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'PET%' and myrpetflag = 'N');
--
-- update {maintable_name} set myrprudentialflag = 'M'
-- where upr_concat_fnlnaddr1 in (
--     select upr_concat_fnlnaddr1 from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'PRUD%' and myrprudentialflag = 'N');


-- Level 2 Match on oldmyrid
update {maintable_name} set myrinforceflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'INFO%' and myrinforceflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myroptoutflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'OPT%' and myroptoutflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrltcflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'LTC%' and myrltcflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrtravelflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'TRAV%' and myrtravelflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrautolibertyflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-LIB%' and myrautolibertyflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrautometflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-MET%' and myrautometflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrautobindableflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'AUTO-BIND%' and myrautobindableflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myradvisoryflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'ADVIS%' and myradvisoryflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrrealestateflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'REAL%' and myrrealestateflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrlaurelroadflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'LAUREL%' and myrlaurelroadflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myridprotectionflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'ID PROT%' and myridprotectionflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrpetflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'PET%' and myrpetflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

update {maintable_name} set myrprudentialflag = 'Y'
where oldmyrid in (
    select oldmyrid from meyer_18702_ids_tobedropped where upr_myrsourcetable like 'PRUD%' and myrprudentialflag = 'N')
  and nvl(myrrecordid) != ''
  and udf_isnumeric(myrschoolid) and myrschoolid > '000';

