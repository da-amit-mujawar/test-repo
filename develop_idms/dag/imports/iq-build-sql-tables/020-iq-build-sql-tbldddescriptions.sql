--create table for loading
drop table if exists {table_dddesc_1};
create table {table_dddesc_1}(
	id integer not null default 0,
	DatabaseID int not null default 0,
	cFieldName varchar(100) null,
	cvalue varchar(20) null,
	cdescription varchar(100) not null default '',
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null)
diststyle all
compound sortkey(DatabaseID,cFieldName);

--load pipe delimited text file with header
copy {table_dddesc_1}
from 's3://{s3-internal}{s3-key10}'
iam_role '{iam}'
acceptinvchars
delimiter '|'
timeformat 'Mon DD YYYY HH:MI:SS';

-- no transform needed

--create table for loading
drop table if exists {table_dddesc_iq_1};
create table {table_dddesc_iq_1}(
	id integer not null default 0,
	DatabaseID int not null default 0,
	cFieldName varchar(100) null,
	cvalue varchar(20) null,
	cdescription varchar(100) not null default '',
	dcreateddate timestamp not null default getdate(),
	ccreatedby varchar(25) not null default '',
	dmodifieddate timestamp null,
	cmodifiedby varchar(25) null)
diststyle all
compound sortkey(DatabaseID,cFieldName);

--load pipe delimited text file with header
copy {table_dddesc_iq_1}
from 's3://{s3-internal}{s3-key10}'
iam_role '{iam}'
acceptinvchars
delimiter '|'
timeformat 'Mon DD YYYY HH:MI:SS';


