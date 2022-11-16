--load pipe delimited gz file from company aop output
copy {table_company}
from 's3://{s3-aopoutput}{s3-key1}'
iam_role '{iam}'
DELIMITER '|' GZIP ACCEPTINVCHARS IGNOREHEADER 1 IGNOREBLANKLINES REMOVEQUOTES MAXERROR 1200;
--22904261


insert into {table_job_stats}
select 'Load Company MC Table',count(*),getdate() from {table_company};

insert into {table_job_stats}
select 'Drop From Company MC Table MS 5',count(*),getdate() from {table_company} where substring(ah1_mailability_score,1,1)='5';

delete from {table_company} where substring(ah1_mailability_score,1,1)='5';
--318160

insert into {table_job_stats}
select 'Drop From Company MC Blanks',count(*),getdate() from {table_company} where ah1_zip_code=' ' or company=' ' or ah1_local_address=' ';

delete from {table_company} where ah1_zip_code=' ' or company=' ' or ah1_local_address=' ';
--5046

insert into {table_job_stats}
select 'Load Company MC Table After Drops',count(*),getdate() from {table_company};

