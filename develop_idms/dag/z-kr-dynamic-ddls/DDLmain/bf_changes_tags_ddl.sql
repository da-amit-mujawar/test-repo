DROP TABLE IF EXISTS core_bf.tags;

CREATE TABLE core_bf.tags
(
infogroup_id varchar(15) NOT NULL SORTKEY,
tags_id Varchar(32) PRIMARY KEY,
created_at varchar(50)  ,
adsize_code varchar(1)  ,
labels_adsize_code Varchar(30)  ,
"Primary" varchar(5)  ,
sic_code_id varchar(10)  ,
labels_sic_code_id varchar(250)  ,
naics_code_id varchar(10)  ,
labels_naics_code_id varchar(250),
yellow_page_code varchar(10)  ,
labels_yellow_page_code varchar(50)  ,
yppa_code varchar(10)  
)
diststyle ALL
;