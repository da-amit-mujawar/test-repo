--business exec 
copy {table_business_exec_raw}
from 's3://{s3-cdbus-path}{s3-business-exec-key11}'
iam_role '{iam}'
FORMAT AS PARQUET;

drop table if exists {table_business_exec};

create table {table_business_exec} as
select cast(seq as varchar (30)) as id,*
   from {table_business_exec_raw};










