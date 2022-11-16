--create table for loading to redshift
drop table if exists {table_sic61};
drop table if exists {table_empsz}_new;
drop table if exists {table_salesvol}_new;
drop table if exists {table_sqftg}_new;
drop table if exists {table_busdept}_new;
drop table if exists {table_buslvl}_new;
drop table if exists {table_busfuncarea}_new;
drop table if exists {table_busrole}_new;

create table {table_sic61} (ccode char(6) not null, cdescription varchar(45) not null);

/* see load sql
create table {table_empsz}_new (ccode varchar(20) not null, cdescription varchar(100) not null);
create table {table_salesvol}_new (ccode varchar(20) not null, cdescription varchar(100) not null);
create table {table_sqftg}_new (ccode varchar(20) not null, cdescription varchar(100) not null);
create table {table_busdept}_new (ccode varchar(20) not null, cdescription varchar(100) not null);
create table {table_buslvl}_new (ccode varchar(20) not null, cdescription varchar(100) not null);
create table {table_busfuncarea}_new (ccode varchar(20) not null, cdescription varchar(100) not null);
create table {table_busrole}_new (ccode varchar(20) not null, cdescription varchar(100) not null);
*/
