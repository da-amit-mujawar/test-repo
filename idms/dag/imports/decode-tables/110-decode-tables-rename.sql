-- replace old table with new
drop table if exists {table_sic62};
drop table if exists {table_empsz};
drop table if exists {table_salesvol};
drop table if exists {table_sqftg};
drop table if exists {table_busdept};
drop table if exists {table_buslvl};
drop table if exists {table_busfuncarea};
drop table if exists {table_busrole};

-- rename new table for use
alter table {table_sic61} rename to {table_sic62};
alter table {table_empsz}_new rename to {table_empsz};
alter table {table_salesvol}_new rename to {table_salesvol};
alter table {table_sqftg}_new rename to {table_sqftg};
alter table {table_busdept}_new rename to {table_busdept};
alter table {table_buslvl}_new rename to {table_buslvl};
alter table {table_busfuncarea}_new rename to {table_busfuncarea};
alter table {table_busrole}_new rename to {table_busrole};
