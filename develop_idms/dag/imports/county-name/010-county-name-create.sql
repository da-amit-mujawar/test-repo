--create table for loading to redshift
drop table if exists {tablename1};

create table {tablename1}
(   cstatecode    varchar (2)  not null,
	ccountycode   varchar (3)  not null,
	ccountyname   varchar (50) not null)
 diststyle all
 compound sortkey (cstatecode,ccountycode);

