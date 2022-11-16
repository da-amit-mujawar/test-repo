DROP TABLE IF EXISTS core_bf.{counts_table} CASCADE;

CREATE TABLE core_bf.{counts_table}
( 
filetype varchar(100),
today date,
noOfRecords varchar(100)
)
;