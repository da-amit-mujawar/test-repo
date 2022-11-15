DROP TABLE IF EXISTS core_bf.intents_changes;

CREATE TABLE core_bf.intents_changes
( 
infogroup_id varchar(15) SORTKEY ENCODE RAW,
id Varchar(32) PRIMARY KEY ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD,
topic_ids Varchar(1000) ENCODE ZSTD,
labels_topic_ids varchar(1000) ENCODE ZSTD,
topic_ids_count integer ENCODE AZ64,
score float ENCODE ZSTD
)
diststyle ALL
;