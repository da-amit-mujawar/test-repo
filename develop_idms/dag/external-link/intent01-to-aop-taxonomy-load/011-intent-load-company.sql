--load pipe delimited gz files

copy {table_company_raw}
from 's3://{s3-intent-path}{s3-key1}'
iam_role '{iam}'
DELIMITER ',' GZIP ACCEPTINVCHARS IGNOREHEADER 1 IGNOREBLANKLINES REMOVEQUOTES MAXERROR 1200;


insert into {table_job_stats}
select 'Load Company Raw Table',count(*),getdate() from {table_company_raw};



