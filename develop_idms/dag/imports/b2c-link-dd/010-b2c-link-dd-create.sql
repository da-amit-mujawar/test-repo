--create table for loading to redshift
drop table if exists {tablename1};

create table {tablename1} (
    databaseid    int           not null,
	cfieldname    varchar (50)  not null,
	cvalue        varchar (20)  not null,
	cdescription  varchar (100) not null)
 diststyle all
 compound sortkey (cfieldname,cvalue);
