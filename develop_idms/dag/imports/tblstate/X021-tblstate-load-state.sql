-- 1. load from raw source table, don't use state name (spelling errors)
insert into {tablename1}
    (cstatecode,ccountycode,ccounty,ccity,czip)
 select distinct stateabbr,
      countynumber as ccountycode,
	  trim(initcap(countyname)) as ccounty,
	  trim(initcap(citystatename)) as ccity,
 	  zipcode as czip
 from {table_input1}
order by stateabbr,czip;

-- 2. Make sure state names are spelled/populated correctly
update {tablename1}
  set  cstate = b.cstate
from {tablename1} a
inner join {table_statename} b on a.cstatecode =b.cstatecode;

-- 3. Add Canada records
insert into {tablename1}
    (cstatecode,cstate,ccountycode,ccounty,ccity,czip)
select distinct cstatecode, cstate, ccountycode, ccounty, ccity, czip
  from {table_canada}
 order by cstatecode, czip;

-- 4. Add DA-US-Business-992 records
insert into {tablename1}
    (cstatecode,cstate,ccountycode,ccounty,ccity,czip,databaseid)
select distinct cstatecode, cstate, ccountycode, ccounty, ccity, czip, databaseid
  from {table_992}
 order by cstatecode, czip;

-- 5. Add DA-Consumer-1267 records
insert into {tablename1}
    (cstatecode,cstate,ccountycode,ccounty,ccity,czip,databaseid)
select distinct cstatecode, cstate, ccountycode, ccounty, ccity, czip, databaseid
  from {table_1267}
 order by cstatecode, czip;

