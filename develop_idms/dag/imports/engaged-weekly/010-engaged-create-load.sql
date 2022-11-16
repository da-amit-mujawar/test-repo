--create stat table
drop table if exists {table_job_stats};

create table {table_job_stats}
(task varchar(150),quantity bigint, run_date timestamp sortkey);


drop table if exists {tablename1};

create table {tablename1}
    (emailaddress varchar(200) not null default '' sortkey,
    open_date varchar(20) not null default '',
    click_date varchar(20) not null default '',
    opened varchar(20) not null default '',
    clicked varchar(20) not null default '',
    optout varchar(20) not null default '');

--load fixed-width file no header
copy {tablename1}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
acceptinvchars
delimiter ','
;

insert into {table_job_stats}
select 'load Edithroman_Engagers_All file',count(*),getdate() from {tablename1};

--create table engaged_domain_db
drop table if exists {tablename2};

create table {tablename2}
    (domain_name varchar(80) not null default '',
    email_count int,
    last_open_date varchar(8) default '',
    last_click_date varchar(8) default '',
    times_opened int,
    times_clicked int);
