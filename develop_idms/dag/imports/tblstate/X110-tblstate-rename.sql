-- replace old table with new
drop table if exists {table_input2};
drop table if exists {tablename2};

-- rename new table for use
alter table {table_input1} rename to {table_input2};
alter table {tablename1} rename to {tablename2};


