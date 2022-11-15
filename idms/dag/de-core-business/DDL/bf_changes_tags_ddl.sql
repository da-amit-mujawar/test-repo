DROP TABLE IF EXISTS core_bf.tags_changes; 

CREATE TABLE core_bf.tags_changes
(
infogroup_id varchar(15) NOT NULL SORTKEY ENCODE RAW,
id Varchar(32)  PRIMARY KEY ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD,
adsize_code varchar(1)  ENCODE ZSTD,
labels_adsize_code Varchar(30)  ENCODE ZSTD,
"Primary" varchar(5)  ENCODE ZSTD,
sic_code_id varchar(10)  ENCODE ZSTD,
labels_sic_code_id varchar(250)  ENCODE ZSTD,
naics_code_id varchar(10)  ENCODE ZSTD,
labels_naics_code_id varchar(250) ENCODE ZSTD,
yellow_page_code varchar(10)  ENCODE ZSTD,
labels_yellow_page_code varchar(50) ENCODE ZSTD,
yppa_code varchar(10) ENCODE ZSTD
)
diststyle ALL
;