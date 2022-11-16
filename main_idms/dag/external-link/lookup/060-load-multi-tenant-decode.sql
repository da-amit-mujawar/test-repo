/*
--Reju M 2019.03.25
*/



DROP TABLE IF EXISTS {tablename11}; 

create table {tablename11} (cCode char(1) SORTKEY PRIMARY KEY, cDescription varchar(5));
insert into {tablename11} values ('A','2 - 4');
insert into {tablename11} values ('B','5 - 9');
insert into {tablename11} values ('C','10+');

