
--listID load tab delimited text file no header
copy {table_sap_listid}
from 's3://{s3-cdbus-path}{s3-key12}'
iam_role '{iam}'
delimiter '\t';

insert into {table_job_stats}
select 'Load Sap ListID File Table',count(*),getdate() from {table_sap_listid};