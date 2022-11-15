--append files after aop, pipe delimited

copy {table_email_appended}
from 's3://{s3-aopoutput}{s3-key10}'
iam_role '{iam}'
DELIMITER '|' GZIP ACCEPTINVCHARS IGNOREBLANKLINES REMOVEQUOTES MAXERROR 1200;

insert into {table_job_stats}
select 'Load Sap Appended Email File Table',count(*),getdate() from {table_email_appended};

--drop bad records
delete from {table_email_appended}
where email not like '%@%';

insert into {table_job_stats}
select 'Qty After Bad Email drops',count(*),getdate() from {table_email_appended};