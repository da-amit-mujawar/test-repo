--create table for loading to redshift
drop table if exists {tablename1};

create table {tablename1}
(
	cphone    varchar (10) not null sortkey
)
;

CREATE TABLE IF NOT EXISTS exclude_donotcallflag
(
    cPhone CHAR(10) SORTKEY PRIMARY KEY,
    cFileDate CHAR(8)
) 
DISTSTYLE ALL;

