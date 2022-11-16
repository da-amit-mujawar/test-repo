-- child table first degree

drop table if exists meyer_first_degree_new;

create table meyer_first_degree_new
(
graduationage varchar(250),
myr1degree varchar(250)
)
diststyle all;


copy meyer_first_degree_new
(
graduationage,
myr1degree
)
from 's3://{s3-cust-meyer}{s3-key3}'
iam_role '{iam}'
delimiter '|'
ignoreheader as 1
;
