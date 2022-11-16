--IDMS-1782: B2C Link Counts Report for Capital One

--load data to work table
drop table if exists #tmpB2CLink;
select --top 1000 
  bus_fulfillmentflag, execureachquality, recordtypeindicator,
  abi_available, titlecode, business_addr_available, bus_state, consumer_addr_available, cons_state, cons_locationtype, cons_creditcardholder, cons_ownrent, 
  bus_companynameavailable, bus_professionaltitle, bus_corporate_employeecount, bus_actual_location_employment_size, bus_actual_location_sales_volume, 
  bus_corporate_salesvolumeamount, bus_contactliteraltitlecode_description, bus_zipfour, cons_unit_number, bus_contactliteraltitlecode,
  bus_countycode, bus_primarysiccode, cons_countycode, bus_phonenumber, cons_zipfour, cons_income
into #tmpB2CLink
  from  {maintable_name}
 where bus_fulfillmentflag = 'V' 
   and execureachquality between '1' and '4';
   

-- create report table
drop table if exists #tmpCapOne;
    create table #tmpCapOne(
        ID int identity not null,
        B2CLink varchar(100),
        FieldValue varchar(100),
        tempdescription varchar(100),
        TotalDatabase int,
        B2C int,
        C2B int,
        Common int        
    );

--SQL for AVAILABLE fields -- sort to top of report, Total Verified s/b first, do not remove preceding space
insert into #tmpCapOne (B2CLink, FieldValue, tempdescription, TotalDatabase, B2C, C2B, Common)
    select  '  TotalVerified', 'V', 'Verified record count', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where bus_fulfillmentflag = 'V'  UNION

    select  ' abi_available', 'Y', 'IGID is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where abi_available = 'Y'  UNION

    select  ' business_addr_available', 'Y', 'Address is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where business_addr_available = 'Y'  UNION

    select  ' consumer_addr_available', 'Y', 'Address is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where consumer_addr_available = 'Y'  UNION

    select  ' bus_companynameavailable', 'Y', 'Name is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where bus_companynameavailable = 'Y'  UNION

--SQL for CALCULATED available fields
    select  ' bus_zipfour_available', 'Y', 'Zip4 is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_zipfour,'') <> ''  UNION

    select  ' bus_professionaltitle_available', 'Y', 'Professional title is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_professionaltitle,'') <> ''  UNION

    select  ' cons_unit_number_available', 'Y', 'Unit number is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(cons_unit_number,'') <> ''  UNION

    select  ' bus_contactliteraltitlecode_available', 'Y', 'Contact titlecode is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_contactliteraltitlecode,'') <> ''  UNION

    select  ' titlecode_available', 'Y', 'Titlecode is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(titlecode,'') <> ''  UNION

    select  ' bus_corporate_employeecount_available', 'Y', 'Employee count is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_corporate_employeecount,'') <> ''  UNION

    select  ' bus_actual_location_employment_size_available', 'Y', 'Employee count is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_actual_location_employment_size,'') <> ''  UNION

    select  ' bus_actual_location_sales_volume_available', 'Y', 'Sales is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_actual_location_sales_volume,'') <> ''  UNION

    select  ' bus_corporate_salesvolumeamount_available', 'Y', 'Sales is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_corporate_salesvolumeamount,'') <> ''  UNION

    select  ' bus_contactliteraltitlecode_description_available', 'Y', 'Description is populated', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_contactliteraltitlecode_description,'') <> ''  UNION

    select  ' bus_countycode_available', 'Y', 'Description is populated', count(*),
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_countycode,'') <> ''  UNION

    select  ' bus_primarysiccode_6digit_available', 'Y', 'Description is populated', count(*),
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_primarysiccode,'') <> ''  UNION

    select  ' cons_countycode_available', 'Y', 'Description is populated', count(*),
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(cons_countycode,'') <> ''  UNION

    select  ' bus_phonenumber_available', 'Y', 'Description is populated', count(*),
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(bus_phonenumber,'') <> ''  UNION

    select  ' cons_zipfour_available', 'Y', 'Description is populated', count(*),
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  where nvl(cons_zipfour,'') <> ''

    order by 1,2;

--SQL for standard fields (sorted after available)
insert into #tmpCapOne (B2CLink, FieldValue, tempdescription, TotalDatabase, B2C, C2B, Common)
    select  'titlecode', titlecode, '', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by titlecode  UNION

    select  'cons_state', cons_state, '', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by cons_state  UNION

    select  'cons_locationtype', cons_locationtype, '', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by cons_locationtype  UNION

    select  'cons_creditcardholder', cons_creditcardholder, '', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by cons_creditcardholder  UNION

    select  'cons_ownrent', cons_ownrent, '', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by cons_ownrent  UNION

    select  'bus_professionaltitle', bus_professionaltitle, '', count(*), 
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by bus_professionaltitle  UNION

    select  'bus_state', bus_state, '', count(*),
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by bus_state  UNION

    select  'cons_income', cons_income, '', count(*),
            SUM(CASE WHEN recordtypeindicator  = 'B' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'C' THEN 1 ELSE 0 END), SUM(CASE WHEN recordtypeindicator  = 'X' THEN 1 ELSE 0 END)
    from #tmpB2CLink  group by cons_income


order by 1,2;


--get dd for descriptions where available
select iaudittype, iisselectable, iallowexport, btl.cfieldname, bdd.cvalue, bdd.cdescription
into #tmpDD
from sql_tblbuild b
inner join sql_tblbuildtable bt on b.id = bt.buildid
inner join sql_tblbuildtablelayout btl on bt.id = btl.buildtableid
inner join sql_tblbuilddd bdd on btl.id = bdd.buildtablelayoutid
where b.id = {build_id}
order by btl.cfieldname, bdd.cvalue;


--combine dd with counts
drop table if exists exclude_B2CLink_CapOneRpt_{build_id};
select id, b2clink, fieldvalue, tempdescription, cfieldname, cvalue, cdescription,
        CASE WHEN nvl(fieldvalue,'') = '' THEN concat(b2clink,' is blank') ELSE
            CASE WHEN nvl(tempdescription,'') = '' and nvl(cdescription,'') = '' THEN fieldvalue ELSE
                CASE WHEN nvl(tempdescription,'') = '' and nvl(cdescription,'') <> '' THEN cdescription ELSE tempdescription 
        END END END fielddescription,
       TotalDatabase, B2C, C2B, Common
into exclude_B2CLink_CapOneRpt_{build_id}
from #tmpCapOne 
left join #tmpDD dd on b2clink = dd.cfieldname and fieldvalue = dd.cvalue;


--unload... 
unload ('select b2clink, fielddescription, TotalDatabase, B2C, C2B, Common from exclude_B2CLink_CapOneRpt_{build_id} order by b2clink, fielddescription, cValue;')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
csv 
header
delimiter '|'
encrypted
allowoverwrite
parallel off
;
