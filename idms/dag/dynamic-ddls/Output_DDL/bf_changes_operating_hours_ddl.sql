DROP TABLE IF EXISTS core_bf.operating_hours_changes;

CREATE TABLE core_bf.operating_hours_changes
( 
infogroup_id varchar(15),
Operating_hours_id Varchar(32) PRIMARY KEY,
created_at varchar(50),
start_time varchar(20),
end_time varchar(20),
"days" Varchar(50),
labels_days Varchar(50),
days_count int
)
diststyle ALL
;