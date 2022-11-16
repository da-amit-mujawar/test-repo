---Create sapphire report

drop table if exists park_multicoding_count;

create table park_multicoding_count (descr varchar(50), cnt int);

insert into park_multicoding_count
select 'MultiBuyerCount_ByEmail',count(*) from count_{order_id}_MultiBuyerCount_ByEmail_park_tobedropped;

insert into park_multicoding_count
select 'InterestArea',count(*) from count_{order_id}_InterestArea_park_tobedropped;

insert into park_multicoding_count
select 'JobFunction',count(*) from count_{order_id}_jobfunction_park_tobedropped;

insert into park_multicoding_count
select 'ProductSpecified',count(*) from count_{order_id}_ProductSpecified_park_tobedropped;

insert into park_multicoding_count
select 'Specialty',count(*) from count_{order_id}_Specialty_park_tobedropped;

insert into park_multicoding_count
select 'HardwareOnSite',count(*) from count_{order_id}_HardwareOnsite_park_tobedropped;

insert into park_multicoding_count
select 'SoftwareOnSite',count(*) from count_{order_id}_SoftwareOnsite_park_tobedropped;

unload ('select * from park_multicoding_count')
to 's3://{s3-internal}{reportname}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off
;


---drop table if exists park_multicoding_count;

