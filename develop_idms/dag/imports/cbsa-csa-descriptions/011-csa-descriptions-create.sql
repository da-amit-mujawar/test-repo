--create sql_ iq table (gross) for loading to redshift --26K
drop table if exists {table_csa_0};

create table {table_csa_0}(
	csacode  varchar(3) not null default '',
	csa_name varchar(60) not null default '',
	filler   varchar(52) null)
diststyle all;

drop table if exists {table_csa_1};

create table {table_csa_1}(
	csacode  varchar(3) not null default '',
	csa_name varchar(60) not null default '',
    iorder   integer)
diststyle all;


/* DBA.EXCLUDE_CSA_Descriptions
cname,coltype,length
CSACODE,varchar,3
CSA_NAME,varchar,60
iOrder,integer,4
 */