-- remove empty values in join columns
delete from {tablename1}
 where trim(cphone) = '';

-- insert new records into production table
insert into {tablename2} (cphone,cfiledate)
select distinct cphone, replace(CURRENT_DATE,'-','')
  from {tablename1}
 where cphone not in (select cphone from {tablename2});

-- delete records from production table
delete from {tablename2}
where not cphone in
    (select cphone from {tablename1});

-- Unload to S3 for other ELT process to use. For example, consumer (1267) and business (992)
UNLOAD ('SELECT cphone from {tablename2}')
TO 's3://{s3-axle-gold}/do-not-call/donotcall_'
IAM_ROLE '{iam}'
CLEANPATH
FORMAT PARQUET;

