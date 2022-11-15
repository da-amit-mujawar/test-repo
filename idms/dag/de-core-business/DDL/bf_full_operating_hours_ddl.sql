DROP TABLE IF EXISTS core_bf.operating_hours_new;

CREATE TABLE core_bf.operating_hours_new
(
infogroup_id varchar(15) ENCODE ZSTD,
id Varchar(32) PRIMARY KEY ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD,
start_time varchar(20) ENCODE BYTEDICT,
end_time varchar(20) ENCODE BYTEDICT,
"days" Varchar(50) ENCODE ZSTD,
labels_days Varchar(50) ENCODE ZSTD,
days_count int ENCODE AZ64
)
diststyle ALL
;