drop table if exists {table_sap_industry};

SELECT DISTINCT B.individual_mc as individualmc, B.Industry as industry_m
into {table_sap_industry}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;

drop table if exists {table_sap_interestarea};

SELECT DISTINCT B.individual_mc as individualmc, B.InterestArea as interest_area
into {table_sap_interestarea}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;

drop table if exists {table_sap_jobfunction};

SELECT DISTINCT B.individual_mc as individualmc, B.JobFunction as job_function
into {table_sap_jobfunction}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;

drop table if exists {table_sap_locationtype};

SELECT DISTINCT B.individual_mc as individualmc, B.LocationType as location_type
into {table_sap_locationtype}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;

drop table if exists {table_sap_productspecified};

SELECT DISTINCT B.individual_mc as individualmc, B.ProductSpecified as product_specified
into {table_sap_productspecified}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;

drop table if exists {table_sap_specialty};

SELECT DISTINCT B.individual_mc as individualmc, B.Specialty
into {table_sap_specialty}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;

drop table if exists {table_sap_hardware};

SELECT DISTINCT B.individual_mc as individualmc, B.HardwareOnsite as technology_on_site_h
into {table_sap_hardware}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;

drop table if exists {table_sap_software};

SELECT DISTINCT B.individual_mc as individualmc, B.SoftwareOnsite as technology_on_site_s
into {table_sap_software}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
ORDER BY 1,2
;


drop table if exists {table_sap_email};

SELECT CAST (B.Listid as varchar(6)),B.individual_mc as individualmc, LEFT(B.EmailAddress,65) as Email
into {table_sap_email}
from public.count_{order_id} A
inner join {maintable_name} B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
---inner join tblMain_23575_202206 B ON B.ID = A.DWID AND a.Individual_ID = B.Individual_ID
where B.EmailAddress <>''
ORDER BY 1,2,3
;

--create stat table
drop table if exists {table_job_stats};

create table {table_job_stats}
(task varchar(150),quantity bigint, run_date timestamp sortkey);

insert into {table_job_stats}
select 'Load Sap Email File Table',count(*),getdate() from {table_sap_email};

--drop bad MCs from email file
delete from {table_sap_email}
where substring(IndividualMC,1,1) ~ '[^[:digit:]]';

insert into {table_job_stats}
select 'Drop Bad MC Sap Email File Table',count(*),getdate() from {table_sap_email};


--table_sap_hardware load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Hardware File Table',count(*),getdate() from {table_sap_hardware};

--table_sap_software load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Software File Table',count(*),getdate() from {table_sap_software};

--table_sap_industry load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Industry File Table',count(*),getdate() from {table_sap_industry};

--table_sap_interestarea load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Interestarea File Table',count(*),getdate() from {table_sap_interestarea};

--table_sap_jobfunction load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Jobfunction File Table',count(*),getdate() from {table_sap_jobfunction};

--table_sap_locationtype load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Locationtype File Table',count(*),getdate() from {table_sap_locationtype};

--table_sap_productspecified load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Productspecified File Table',count(*),getdate() from {table_sap_productspecified};


--table_sap_specialty load pipe delimited text file no header

insert into {table_job_stats}
select 'Load Sap Specialty File Table',count(*),getdate() from {table_sap_specialty};


