--create table for loading
drop table if exists {table_cbsa_0};

create table {table_cbsa_0}(
	cbsacode varchar(5) not null default '',
	cbtitle  varchar(50) not null default '',
    filler   varchar(23) null)
diststyle all;

drop table if exists {table_cbsa_1};

create table {table_cbsa_1}(
	cbsacode varchar(5) not null default '',
	cbtitle  varchar(50) not null default '',
    iorder   integer)
diststyle all;

/* dba.exclude_cbsa_descriptions
cname,coltype,length
cbsacode,varchar,5
cbtitle,varchar,50
iorder,integer,4
 */
