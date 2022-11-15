--table_sap_hardware load pipe delimited text file no header
copy {table_sap_hardware}
from 's3://{s3-cdbus-path}{s3-key13}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Hardware File Table',count(*),getdate() from {table_sap_hardware};


--table_sap_software load pipe delimited text file no header
copy {table_sap_software}
from 's3://{s3-cdbus-path}{s3-key14}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Software File Table',count(*),getdate() from {table_sap_software};

--table_sap_industry load pipe delimited text file no header
copy {table_sap_industry}
from 's3://{s3-cdbus-path}{s3-key15}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Industry File Table',count(*),getdate() from {table_sap_industry};

--table_sap_interestarea load pipe delimited text file no header
copy {table_sap_interestarea}
from 's3://{s3-cdbus-path}{s3-key16}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Interestarea File Table',count(*),getdate() from {table_sap_interestarea};

--table_sap_jobfunction load pipe delimited text file no header
copy {table_sap_jobfunction}
from 's3://{s3-cdbus-path}{s3-key17}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Jobfunction File Table',count(*),getdate() from {table_sap_jobfunction};

--table_sap_locationtype load pipe delimited text file no header
copy {table_sap_locationtype}
from 's3://{s3-cdbus-path}{s3-key18}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Locationtype File Table',count(*),getdate() from {table_sap_locationtype};

--table_sap_productspecified load pipe delimited text file no header
copy {table_sap_productspecified}
from 's3://{s3-cdbus-path}{s3-key19}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Productspecified File Table',count(*),getdate() from {table_sap_productspecified};


--table_sap_specialty load pipe delimited text file no header
copy {table_sap_specialty}
from 's3://{s3-cdbus-path}{s3-key20}'
iam_role '{iam}'
FORMAT AS PARQUET;

insert into {table_job_stats}
select 'Load Sap Specialty File Table',count(*),getdate() from {table_sap_specialty};