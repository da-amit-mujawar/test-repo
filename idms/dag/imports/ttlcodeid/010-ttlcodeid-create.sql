--create table for loading to redshift
drop table if exists {tablename1};

create table {tablename1}(
    titleid           varchar (8)   not null,
	titledescription  varchar (256) not null)
;
