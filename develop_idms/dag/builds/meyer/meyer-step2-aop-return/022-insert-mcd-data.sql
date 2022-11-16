-- Step 5: Export MCD File as per MCD Input Layout
-- Remove special characters: ;$&~",\?
-- (s3://idms-7933-aop-input/IDMSAOPPI1/a05_meyer_mcd_input.txt)

insert into meyer_mcd_input
    (sequence,file_no,run,enterpriseid,collection,channel,title,first,middle,last,generational,gender,dob,
     address,secondary,unit,city,province,postal,plus4,account,phone,email,company,division,since,mailability)
select
    concat('1',right(concat('000000000',a.id),9)) as sequence,
    b.file_id as file_no,
    '' as run,
    b.mcdindividualid as enterpriseid, --null
    b.mcdhouseholdid as collection, --null
    '1'	as channel,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.daalumnititle,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as title,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.firstname,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as first,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.middleinitial,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as middle,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.lastname,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as last,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.suffix,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as generational,
    '' as gender,
    '' as dob,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.addressline1,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,100) as address,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.addressline2,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,100) as secondary,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.ah1unitinfo,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,10) as unit,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.city,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,50) as city,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.state,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as province,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.zip,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,6) as postal,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.zip4,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,4) as plus4,
    '' as account,
    '' as phone,
    '' as email,
    '' as company,
    '' as division,
    '' as since,
    '' as mailability
from {pre_maintable_name} a inner join
    meyer_return_aop b on a.id = b.id
where b.file_id in ('100')
order by a.id;


insert into meyer_mcd_input
    (sequence,file_no,run,enterpriseid,collection,channel,title,first,middle,last,generational,gender,dob,
     address,secondary,unit,city,province,postal,plus4,account,phone,email,company,division,since,mailability)
select
    concat('2',right(concat('000000000',a.id),9)) as sequence,
    b.file_id as file_no,
    '' as run,
    b.mcdindividualid as enterpriseid,
    b.mcdhouseholdid as collection,
    '1'	as channel,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.daalumnititle,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as title,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.firstname,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as first,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.middleinitial,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as middle,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.lastname,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as last,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.suffix,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as generational,
    '' as gender,
    '' as dob,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.addressline1,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,100) as address,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.addressline2,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,100) as secondary,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.ah1unitinfo,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,10) as unit,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.city,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,50) as city,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.state,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,30) as province,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.zip,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,6) as postal,
    substring(replace(replace(replace(replace(replace(replace(replace(replace(nvl(b.zip4,''),'~',''),';',' '),'$',' '),'&',' '),'"',' '),',',' '),'\\',' '),'?',' '),1,4) as plus4,
    '' as account,
    '' as phone,
    '' as email,
    '' as company,
    '' as division,
    '' as since,
    '' as mailability
from {maintable_name} a inner join
    meyer_return_aop b on a.id = b.id
where b.file_id in ('200')
order by a.id;
