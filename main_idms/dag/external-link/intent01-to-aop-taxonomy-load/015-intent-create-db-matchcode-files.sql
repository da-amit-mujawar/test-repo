--create topic table for loading to redshift

drop table if exists {table_b2c_link1};
drop table if exists {table_b2c_link};


create table {table_b2c_link1} (
company_mc varchar(15) SORTKEY DISTKEY
);


create table {table_b2c_link} (
company_mc varchar(15) SORTKEY DISTKEY UNIQUE
);


drop table if exists {table_sapphire1};
drop table if exists {table_sapphire};

create table {table_sapphire1} (
company_mc varchar(15) SORTKEY DISTKEY
);

create table {table_sapphire} (
company_mc varchar(15) SORTKEY DISTKEY UNIQUE
);


drop table if exists {table_us_bus};
 
create table {table_us_bus} (
company_mc varchar(15) SORTKEY DISTKEY UNIQUE
);
