DROP TABLE IF EXISTS core_bf.happy_hours_changes;

CREATE TABLE core_bf.happy_hours_changes
( 
infogroup_id varchar(15) ENCODE ZSTD,
id Varchar(32) PRIMARY KEY ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD,
special_food Varchar(5) ENCODE ZSTD,
special_drink Varchar(5) ENCODE ZSTD,
special_activity Varchar(5) ENCODE ZSTD,
special_other Varchar(5) ENCODE ZSTD,
start_time varchar(20) ENCODE ZSTD,
end_time varchar(20) ENCODE ZSTD,
"days" Varchar(50) ENCODE ZSTD,
labels_days Varchar(50) ENCODE ZSTD,
days_count int ENCODE AZ64,
description Varchar(10000) ENCODE ZSTD
)
diststyle ALL
;