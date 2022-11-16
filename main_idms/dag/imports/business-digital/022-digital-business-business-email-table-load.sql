--business email 
copy {table_business_email_raw}
from 's3://{s3-cdbus-path}{s3-business-email-key11}'
iam_role '{iam}'
FORMAT AS PARQUET;

drop table if exists {table_business_email};

create table {table_business_email} as
select cast(seq as varchar(30)) id,*
    from {table_business_email_raw};












