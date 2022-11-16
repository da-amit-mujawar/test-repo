DROP TABLE IF EXISTS core_bf.explains_new;

CREATE TABLE core_bf.explains_new
(
infogroup_id varchar(15),
id varchar(32) PRIMARY KEY,
created_at varchar(50),
system_name varchar(100),
labels_system_name varchar(100),
verified_at varchar(50),
deleted_at varchar(50),
filled_at varchar(50),
modified_at varchar(50),
touched_at varchar(50),
verifying_source_code varchar(50),
updating_source_code varchar(50),
field varchar(50)
)
diststyle ALL
;