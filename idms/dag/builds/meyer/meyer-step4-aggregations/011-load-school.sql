-- child table school file

drop table if exists meyer_school_new ;

create table meyer_school_new
(
myrschoolid varchar(250),
myr_school_name varchar(250),
myr_school_alums_address varchar(250),
myr_school_alums_address_lowercase varchar(250),
myr_school_from_email_address varchar(250),
myr_school_from_email_name varchar(250),
myr_school_salutation_type varchar(250) default 'FORMAL',
myr_school_priority varchar(250) default '999',
myr_school_grad_yr_fullname_flag varchar(250),
myr_school_grad_yr_spouse_fullname_flag varchar(250),
myr_school_pro_suffix_no_title_fullname_flag varchar(250),
myr_school_pro_suffix_no_title_spouse_fullname_flg varchar(250),
myr_school_maiden_name_fullname_flag varchar(250),
myr_school_maiden_name_spouse_fullname_flag varchar(250),
myr_school_parse_fullname varchar(250),
myr_school_parse_spouse_fullname varchar(250),
myr_use_school_provided_salutation varchar(250),
myr_use_school_provided_joint_salutation varchar(250),
myr_school_suppress_spouse varchar(250)
)
diststyle all;

copy meyer_school_new
(
myrschoolid,
myr_school_name ,
myr_school_alums_address,
myr_school_alums_address_lowercase,
myr_school_from_email_address,
myr_school_from_email_name,
myr_school_salutation_type,
myr_school_priority,
myr_school_grad_yr_fullname_flag,
myr_school_grad_yr_spouse_fullname_flag,
myr_school_pro_suffix_no_title_fullname_flag,
myr_school_pro_suffix_no_title_spouse_fullname_flg,
myr_school_maiden_name_fullname_flag,
myr_school_maiden_name_spouse_fullname_flag,
myr_school_parse_fullname,
myr_school_parse_spouse_fullname,
myr_use_school_provided_salutation,
myr_use_school_provided_joint_salutation,
myr_school_suppress_spouse
)
from 's3://{s3-cust-meyer}{s3-key1}'
iam_role '{iam}'
delimiter '|'
ignoreheader as 1
acceptinvchars
removequotes;
