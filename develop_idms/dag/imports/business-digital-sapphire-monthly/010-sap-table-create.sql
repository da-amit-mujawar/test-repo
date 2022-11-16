--create stat table

drop table if exists {table_job_stats};

create table {table_job_stats}
(task varchar(150),quantity bigint, run_date timestamp sortkey);



