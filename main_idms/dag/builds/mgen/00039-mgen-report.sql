--create mgen report

drop table if exists mgen_count_{build_id}_{build} CASCADE;

create table mgen_count_{build_id}_{build} (descr varchar(50),cnt int);


insert into mgen_count_{build_id}_{build}
select 'Mgen Main',count(*) from tblmain_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen HHLD',count(*) from tblchild1_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen DQI',count(*) from tblchild2_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen Experian',count(*) from tblchild3_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen Profile',count(*) from tblchild6_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen Apogee',count(*) from tblchild34_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen Transaction',count(*) from tblchild5_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen Transaction SC',count(*) from tblchild4_{build_id}_{build};

insert into mgen_count_{build_id}_{build}
select 'Mgen PUB2',count(*) from mgen_pub2;

insert into mgen_count_{build_id}_{build}
select 'Mgen Target Ready Models',count(*) from mgen_arr_twm;

insert into mgen_count_{build_id}_{build}
select 'Mgen DLX Segments',count(*) from mgen_dlx_segments;


unload ('select * from mgen_count_{build_id}_{build}')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv
allowoverwrite
header
parallel off
;