-- child table generic salutation

drop table if exists meyer_generic_salutation_new;

create table meyer_generic_salutation_new
(
myrfinalalumnimarried varchar(250),
myr_final_alumni_gender varchar(250),
myr_final_spouse_gender varchar(250),
myr_generic_salutation varchar(250)
)
diststyle all;


copy meyer_generic_salutation_new
(
myrfinalalumnimarried,
myr_final_alumni_gender,
myr_final_spouse_gender,
myr_generic_salutation
)
from 's3://{s3-cust-meyer}{s3-key4}'
iam_role '{iam}'
delimiter '|'
ignoreheader as 1
;
