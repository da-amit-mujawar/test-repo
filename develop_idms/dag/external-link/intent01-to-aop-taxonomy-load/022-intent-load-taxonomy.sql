--load pipe delimited gz file from topic manifest

copy {table_taxonomy}
from 's3://{s3-internal}{s3-key7}'
iam_role '{iam}'
DELIMITER '|' ACCEPTINVCHARS IGNOREHEADER 1 IGNOREBLANKLINES REMOVEQUOTES;



copy {table_taxonomy_monthly}
from 's3://{s3-intent-path}{s3-key3}'
iam_role '{iam}'
DELIMITER ',' ACCEPTINVCHARS IGNOREHEADER 1 IGNOREBLANKLINES REMOVEQUOTES;

alter table {table_taxonomy_monthly} add main_cat_code varchar(2);
alter table {table_taxonomy_monthly} add sub_cat_code varchar(5);
alter table {table_taxonomy_monthly} add topic_Code varchar(5);

update {table_taxonomy_monthly}
set main_cat_code={table_taxonomy}.main_cat_code
from {table_taxonomy}
where upper({table_taxonomy_monthly}.main_cat)=upper({table_taxonomy}.main_cat);


update {table_taxonomy_monthly}
set sub_cat_code={table_taxonomy}.sub_cat_code
from {table_taxonomy}
where upper({table_taxonomy_monthly}.sub_cat)=upper({table_taxonomy}.sub_cat);



update {table_taxonomy_monthly}
set topic_code={table_taxonomy}.topic_code
from {table_taxonomy}
where upper({table_taxonomy_monthly}.topic)=upper({table_taxonomy}.topic);


insert into {table_job_stats}
select 'Load Taxonomy Lookup Table',count(*),getdate() from {table_taxonomy};


insert into {table_job_stats}
select 'Load Taxonomy Monthly Table',count(*),getdate() from {table_taxonomy_monthly};

