--load pipe delimited gz file from topic manifest

copy {table_topic}
from 's3://{s3-intent-path}{s3-key2}'
iam_role '{iam}'
DELIMITER ',' GZIP ACCEPTINVCHARS IGNOREHEADER 1 IGNOREBLANKLINES REMOVEQUOTES;

----36123221435 1 h 49 min

insert into {table_job_stats}
select 'Load Topic Table',count(*),getdate() from {table_topic};
