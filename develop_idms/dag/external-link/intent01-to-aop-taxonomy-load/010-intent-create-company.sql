--create company table for loading to redshift
insert into {table_job_stats} values
('Start Intent Monthly Load',0,getdate() );


drop table if exists {table_company_raw};

create table {table_company_raw}
(
account_link_id varchar(100) PRIMARY KEY,
company varchar(150),
address varchar(100),
city varchar(30),
st varchar(2),
zip varchar(10)
);

