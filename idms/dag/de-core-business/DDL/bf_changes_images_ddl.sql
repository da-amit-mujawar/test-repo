DROP TABLE IF EXISTS core_bf.images_changes;

CREATE TABLE core_bf.images_changes
(
infogroup_id varchar(15) ENCODE ZSTD,
id Varchar(32) PRIMARY KEY ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD,
asset_url Varchar(100) ENCODE ZSTD,
asset_hash Varchar(40) ENCODE ZSTD ,
"primary" Varchar(5) ENCODE ZSTD
)
diststyle ALL
;