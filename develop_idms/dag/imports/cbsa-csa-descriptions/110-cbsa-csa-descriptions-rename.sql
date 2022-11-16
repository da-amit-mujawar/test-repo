-- drop temp tables
drop table if exists {table_cbsa_0};
drop table if exists {table_csa_0};

-- cb 2022.03.01: other objects depend on table, backup instead of renaming
/*
-- drop current table
drop table if exists {table_cbsa_2};
drop table if exists {table_csa_2};
select * into
-- rename new table for use
alter table {table_cbsa_1} rename to {table_cbsa_2};
alter table {table_csa_1} rename to {table_csa_2};
*/

-- other objects depend on tables, backup instead of renaming:
drop table if exists {table_cbsa_2}_backup;
select * into {table_cbsa_2}_backup from {table_cbsa_2};

delete from {table_cbsa_2};
insert into {table_cbsa_2} (cbsacode, cbtitle, iorder)
    select cbsacode, cbtitle, iorder from {table_cbsa_1};

drop table if exists {table_csa_2}_backup;
select * into {table_csa_2}_backup from {table_csa_2};

delete from {table_csa_2};
insert into {table_csa_2} (csacode, csa_name, iorder)
    select csacode, csa_name, iorder from {table_csa_1};

