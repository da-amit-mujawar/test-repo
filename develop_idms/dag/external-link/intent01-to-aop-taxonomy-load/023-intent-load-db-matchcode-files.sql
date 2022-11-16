--load DB-matchcode files

--db992 - us-business
copy {table_us_bus}
from 's3://{s3-internal}{s3-key6}'
iam_role '{iam}'
DELIMITER '|' ACCEPTINVCHARS;

insert into {table_job_stats}
select 'Load US Bus MC Table',count(*),getdate() from {table_us_bus};

--db71 - sapphire
copy {table_sapphire1}
from 's3://{s3-internal}{s3-key5}'
iam_role '{iam}'
DELIMITER '|' ACCEPTINVCHARS;


--db71 - remove duplicates
insert into {table_sapphire} (company_mc) select company_mc from {table_sapphire1} group by company_mc;


insert into {table_job_stats}
select 'Load Sapp MC Table',count(*),getdate() from {table_sapphire};

--db1150 - b2c link
copy {table_b2c_link1}
from 's3://{s3-internal}{s3-key4}'
iam_role '{iam}'
DELIMITER '|' ACCEPTINVCHARS;


--db1150 - remove duplicates
insert into {table_b2c_link} (company_mc) select company_mc from {table_b2c_link1} group by company_mc;


insert into {table_job_stats}
select 'Load B2C Link MC Table',count(*),getdate() from {table_b2c_link};

insert into {table_job_stats} values
('Dag1 done',0,getdate() );
