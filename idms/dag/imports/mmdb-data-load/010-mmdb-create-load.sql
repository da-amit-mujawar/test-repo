--create stat table
drop table if exists {table_job_stats};

create table {table_job_stats}
(task varchar(150),quantity bigint, run_date timestamp sortkey);


drop table if exists {tablename1};

create table {tablename1}
(b_abinumber varchar(9), b_countrycode varchar(2), b_state varchar(2), b_locationemployeesize varchar(1), b_fulfillmentflag varchar(1), u_urltypecode varchar(2));


--load csv file no header
copy {tablename1}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
acceptinvchars
delimiter ','
IGNOREBLANKLINES REMOVEQUOTES MAXERROR 50 gzip
;

insert into {table_job_stats}
select 'load Business File',count(*),getdate() from {tablename1};


drop table if exists {tablename2};

create table {tablename2}
(
b_abinumber varchar(9),
e_contactid varchar(12),
e_individualid varchar(12),
e_contactflag varchar(2),
e_contactquality varchar(1),
e_email_deliverable varchar(1),
e_emailflag varchar(2),
e_vendorid2 varchar(2),
e_proftitle varchar(3),
e_titlecode varchar(1),
e_executivesourcecode varchar(2),
e_management_level varchar(22),
e_job_function_id varchar(3),
e_emailsuppress varchar(1),
e_typecode varchar(1),
e_lastname varchar(14),
e_rolecode varchar(4)
)
;


--load csv file
copy {tablename2}
from 's3://{s3-internal}{s3-key2}'
iam_role '{iam}'
acceptinvchars
delimiter ','
IGNOREBLANKLINES REMOVEQUOTES MAXERROR 50 gzip
;

insert into {table_job_stats}
select 'load Executive File',count(*),getdate() from {tablename2};


drop table if exists {tablename3};

create table {tablename3}
(
abinumber varchar(9),
s_siccode varchar(6),
s_primarysicdesignator varchar(1)
)
;


--load csv file
copy {tablename3}
from 's3://{s3-internal}{s3-key3}'
iam_role '{iam}'
acceptinvchars
delimiter ','
IGNOREBLANKLINES REMOVEQUOTES MAXERROR 50 gzip
;

insert into {table_job_stats}
select 'load SIC File',count(*),getdate() from {tablename3};


drop table if exists {tablename4};

create table {tablename4}
(
Abinumber varchar(9),
email_deliverable varchar(1),
vendorid2 varchar(2),
contactid varchar(12),
executivesourcecode varchar(2),
suppression_type varchar(1),
bit_email_suppress varchar(1)
)
;

--load csv file
copy {tablename4}
from 's3://{s3-internal}{s3-key4}'
iam_role '{iam}'
acceptinvchars
delimiter ','
IGNOREBLANKLINES REMOVEQUOTES MAXERROR 50 gzip
;

insert into {table_job_stats}
select 'load Email File',count(*),getdate() from {tablename4};

