-- replace old table with new
drop table if exists {tablename2};

-- rename new table for use
alter table {tablename1} rename to {tablename2};

