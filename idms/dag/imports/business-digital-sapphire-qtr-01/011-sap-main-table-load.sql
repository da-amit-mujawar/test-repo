--sap-main load pipe delimited text file no header
copy {table_sap_main}
from 's3://{s3-cdbus-path}{s3-key10}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Main File Table',count(*),getdate() from {table_company_raw};

delete from {table_sap_main} where zip is NULL;

insert into {table_job_stats}
select 'Drop NULL Zipcodes from Sap Main File Table',count(*),getdate() from {table_company_raw};

delete from {table_sap_main}
where substring(IndividualMC,1,1) ~ '[^[:digit:]]';

insert into {table_job_stats}
select 'Drop Bad IndividualMC from Sap Main File Table',count(*),getdate() from {table_company_raw};
