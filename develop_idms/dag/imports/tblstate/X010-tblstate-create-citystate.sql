--create table for loading to redshift
drop table if exists {table_input1};

create table {table_input1}(
    filler1 varchar(1) not null default '' ,
    zipcode varchar(5) not null default '' ,
    citystatekey varchar(6) not null default '' sortkey,
    zipclassificationcode varchar(1) not null default '' ,
    citystatename varchar(28) not null default '' ,
    citystatenameabbr varchar(13) not null default '' ,
    citystatenamefacilitycode varchar(1) not null default '' ,
    citystatemailingnameindicator varchar(1) not null default '' ,
    preferredlastlinecitystatekey varchar(6) not null default '' ,
    preferredlastlinecitystatename varchar(28) not null default '' ,
    citydeliveryindicator varchar(1) not null default '' ,
    crrs_merged5digitind varchar(1) not null default '' ,
    uniquezipcode varchar(1) not null default '' ,
    financenumber varchar(6) not null default '' ,
    stateabbr varchar(2) not null default '' ,
    countynumber varchar(3) not null default '' ,
    countyname varchar(25) not null default '')
diststyle all;
