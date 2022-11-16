
--create listID table
drop table if exists {table_sap_listid};

create table {table_sap_listid}
( listid varchar(15));


--listID load tab delimited text file no header
copy {table_sap_listid}
from 's3://digital-7933-business{s3-key12}'
iam_role '{iam}'
delimiter '\t';

insert into {table_job_stats}
select 'Load Sap ListID File Table',count(*),getdate() from {table_sap_listid};

