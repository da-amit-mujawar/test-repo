--create table for loading to redshift
drop table if exists {tablename1};

create table {tablename1}
(   ccode          varchar (5)  not null,
	cdescription   varchar (50) not null)
 diststyle all
 sortkey (ccode);

