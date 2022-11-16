---Create sapphire report

drop table if exists sappqtrly_count;

create table sappqtrly_count (descr varchar(50), cnt int);

insert into sappqtrly_count
select 'Industry',count(*) from {table_sap_industry};

insert into sappqtrly_count
select 'InterestArea',count(*) from {table_sap_interestarea};

insert into sappqtrly_count
select 'JobFunction',count(*) from {table_sap_jobfunction};

insert into sappqtrly_count
select 'LocationType',count(*) from {table_sap_locationtype};

insert into sappqtrly_count
select 'ProductSpecified',count(*) from {table_sap_productspecified};

insert into sappqtrly_count
select 'Specialty',count(*) from {table_sap_specialty};

insert into sappqtrly_count
select 'HardwareOnSite',count(*) from {table_sap_hardware};

insert into sappqtrly_count
select 'SoftwareOnSite',count(*) from {table_sap_software};

unload ('select * from sappqtrly_count')
to 's3://{s3-internal}{reportname}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off
;



