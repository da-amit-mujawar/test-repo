--create cigna report

drop table if exists cigna_count_{build_id}_{build};

create table cigna_count_{build_id}_{build} (descr varchar(150),cnt int);


insert into cigna_count_{build_id}_{build}
select 'Cigna Main Table Count',count(*) from {maintable_name};


unload ('select * from cigna_count_{build_id}_{build}')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv
allowoverwrite
header
parallel off
;

unload ('select * from {maintable_name} where id in (select id from {maintable_name} limit 100)')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv
allowoverwrite
header
parallel off
;