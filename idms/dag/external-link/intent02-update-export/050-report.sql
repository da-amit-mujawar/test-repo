--create final report
drop table if exists {table_intent_count};

CREATE TABLE {table_intent_count}

(
code varchar(100),
code_descr varchar(200),
cnt bigint,
cat_sub_topic varchar(100),
File_code varchar(50),
build varchar(50)
);

insert into {table_intent_count}
select main_cat_code,'',count(*),'Main Category','Total Intent',LEFT(CURRENT_DATE, 7)
from {table_topic_final} group by main_cat_code order by main_cat_code;
insert into {table_intent_count}
select sub_cat_code,'',count(*),'Sub Category','Total Intent',LEFT(CURRENT_DATE, 7)
from {table_topic_final} group by sub_cat_code order by sub_cat_code;
insert into {table_intent_count}
select '',topic,count(*),'Topic','Total Intent',LEFT(CURRENT_DATE, 7)
from {table_topic_final} group by topic order by topic;



insert into {table_intent_count}
select main_cat_code,'',count(*),'Main Category','US Bus DB',LEFT(CURRENT_DATE, 7)
from {table_intent_us_bus} group by main_cat_code order by main_cat_code;
insert into {table_intent_count}
select sub_cat_code,'',count(*),'Sub Category','US Bus DB',LEFT(CURRENT_DATE, 7)
from {table_intent_us_bus} group by sub_cat_code order by sub_cat_code;
insert into {table_intent_count}
select '',topic,count(*),'Topic','US Bus DB',LEFT(CURRENT_DATE, 7)
from {table_intent_us_bus} group by topic order by topic;


insert into {table_intent_count}
select main_cat_code,'',count(*),'Main Category','Execureach',LEFT(CURRENT_DATE, 7)
from {table_intent_b2c} group by main_cat_code order by main_cat_code;
insert into {table_intent_count}
select sub_cat_code,'',count(*),'Sub Category','Execureach',LEFT(CURRENT_DATE, 7)
from {table_intent_b2c} group by sub_cat_code order by sub_cat_code;
insert into {table_intent_count}
select '',topic,count(*),'Topic','Execureach',LEFT(CURRENT_DATE, 7)
from {table_intent_b2c} group by topic order by topic;


insert into {table_intent_count}
select main_cat_code,'',count(*),'Main Category','Sapphire',LEFT(CURRENT_DATE, 7)
from {table_intent_sapp} group by main_cat_code order by main_cat_code;
insert into {table_intent_count}
select sub_cat_code,'',count(*),'Sub Category','Sapphire',LEFT(CURRENT_DATE, 7)
from {table_intent_sapp} group by sub_cat_code order by sub_cat_code;
insert into {table_intent_count}
select '',topic,count(*),'Topic','Sapphire',LEFT(CURRENT_DATE, 7)
from {table_intent_sapp} group by topic order by topic;

--add main cat description
update {table_intent_count}
set code_descr=initcap(b.main_cat)
from {table_intent_count} a join {table_taxonomy} b
on b.main_cat_code=a.code
where a.cat_sub_topic='Main Category' ;

--add sub category description
update {table_intent_count}
set code_descr=initcap(b.sub_cat)
from {table_intent_count} a join {table_taxonomy} b
on b.sub_cat_code=a.code
where a.cat_sub_topic='Sub Category' ;

--add code for topic an d update description
update {table_intent_count}
set code_descr=initcap(code_descr);

update {table_intent_count}
set code=b.topic_code
from {table_intent_count} a join {table_taxonomy} b
on replace(upper(b.topic),'-',' ')=replace(upper(a.code_descr),'-',' ')
where a.cat_sub_topic='Topic' ;

drop table if exists {table_intent_count_total};

CREATE TABLE {table_intent_count_total}
(
file_descr varchar(100),
cnt bigint
);

insert into {table_intent_count_total}
select 'Sapphire',count(distinct company_mc) from {table_intent_sapp};
insert into {table_intent_count_total}
select 'US Bus DB',count(distinct company_mc) from {table_intent_us_bus};
insert into {table_intent_count_total}
select 'Execureach',count(distinct company_mc) from {table_intent_b2c};


unload ('select * from {table_intent_count} order by file_code,cat_sub_topic,code_descr')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
header
parallel off
;

unload ('select * from {table_intent_count_total}')
to 's3://{s3-internal}{reportname3}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
header
parallel off
;


--create topic DD file
drop table if exists tblintent_dd;

CREATE TABLE tblintent_dd as (
select distinct replace(topic,'-',' ') as topic from {table_topic_final});

unload ('select initcap(topic) as topic from tblintent_dd order by topic')
to 's3://{s3-internal}{s3-key10}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
fixedwidth '0:100'
ALLOWOVERWRITE
parallel off
;

insert into {table_job_stats} values
('Dag2 done',0,getdate() );
/*
unload ('select * from {table_job_stats} where run_date  between getdate()-90 and getdate() order by run_date desc')
to 's3://{s3-internal}{reportname4}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
header
parallel off
;
*/


drop table {table_company_raw};
drop table {table_company};
drop table {table_topic_mc};
drop table {table_topic_coded};
drop table {table_sapphire1};
drop table {table_sapphire};
drop table {table_us_bus};
drop table {table_b2c_link1};
drop table {table_b2c_link};
drop table {table_topic};