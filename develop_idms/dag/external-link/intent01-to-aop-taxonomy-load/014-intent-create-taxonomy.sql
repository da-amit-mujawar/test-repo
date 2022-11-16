--create topic table for loading to redshift

drop table if exists {table_taxonomy_monthly};

create table {table_taxonomy_monthly}
(
  main_cat varchar(500),
  sub_cat varchar(500),
  topic varchar(500) primary key
);


DROP TABLE IF EXISTS tblintent_taxonomy;

CREATE TABLE {table_taxonomy}
(
  main_cat varchar(500),
  sub_cat varchar(500),
  topic varchar(500) primary key,
  main_cat_code varchar(5),
  Sub_cat_code varchar(5),
  topic_code varchar(5)
);