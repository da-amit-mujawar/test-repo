/*
--create stat table
drop table if exists {table_job_stats};

create table {table_job_stats}
(task varchar(150),quantity bigint, run_date timestamp sortkey);
*/
--create email table
drop table if exists {table_sap_email};

create table {table_sap_email}
(listid varchar(100),
individualmc varchar(50),
email varchar(100),
filler1 varchar(1),
id bigint
);

