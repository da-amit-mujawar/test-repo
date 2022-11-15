DROP TABLE IF EXISTS core_bf.intents_new;

CREATE TABLE core_bf.intents_new
( 
infogroup_id varchar(15) SORTKEY,
Location_intent_id Varchar(32) PRIMARY KEY,
created_at varchar(50),
topic_ids Varchar(1000),
labels_topic_ids varchar(1000),
topic_ids_count integer,
score float
)
diststyle ALL
;