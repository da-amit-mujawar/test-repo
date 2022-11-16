-- drop
drop table if exists {tablename2};
drop table if exists {tabledistinct2};

-- rename new table for use
alter table {tablename1} rename to {tablename2};
alter table {tabledistinct1} rename to {tabledistinct2};


