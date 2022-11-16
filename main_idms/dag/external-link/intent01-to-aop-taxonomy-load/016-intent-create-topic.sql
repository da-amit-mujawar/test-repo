--create topic table for loading to redshift
drop table if exists {table_topic};

create table {table_topic}
(
  account_link_id varchar(100) primary key,
  topic varchar(100),
  score varchar(50)
);

