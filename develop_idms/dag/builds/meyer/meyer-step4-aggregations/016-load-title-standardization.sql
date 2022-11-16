-- child table standardized title file

drop table if exists meyer_title_standardization_new;

create table meyer_title_standardization_new
(
title_code varchar(250),
standardized_title_code varchar(250)
)
diststyle all;

copy meyer_title_standardization_new
(
title_code,
standardized_title_code
)
from 's3://{s3-cust-meyer}{s3-key6}'
iam_role '{iam}'
delimiter '|'
ignoreheader as 1
removequotes
;
