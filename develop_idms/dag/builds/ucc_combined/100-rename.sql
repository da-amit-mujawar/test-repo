--drop input tables
DROP TABLE IF EXISTS {tablename1};
DROP TABLE IF EXISTS {tablename2};

--Final step  Rename table
drop table if exists {maintable_name};
ALTER  TABLE {tablename4} RENAME TO {maintable_name};

