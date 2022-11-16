DROP TABLE IF EXISTS core_bf.happy_hours;

CREATE TABLE core_bf.happy_hours
( 
infogroup_id varchar(15),
Happy_hours_id Varchar(32) PRIMARY KEY,
created_at varchar(50),
special_food Varchar(5),
special_drink Varchar(5),
special_activity Varchar(5),
special_other Varchar(5),
start_time varchar(20),
end_time varchar(20),
days Varchar(50),
labels_days Varchar(50),
days_count int,
description Varchar(10000)
)
diststyle ALL
;