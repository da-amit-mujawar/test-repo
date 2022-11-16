-- lookup state nylmet flag

drop table if exists meyer_state_availability_new;

create table meyer_state_availability_new
(
state varchar(250),
nyl_met_flag varchar(250)
)
diststyle all;


copy meyer_state_availability_new
(
state,
nyl_met_flag
)
from 's3://{s3-cust-meyer}{s3-key5}'
iam_role '{iam}'
delimiter '|'
ignoreheader as 1
;
