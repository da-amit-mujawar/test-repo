DROP TABLE IF EXISTS core_bf.images;

CREATE TABLE core_bf.images
(
infogroup_id varchar(15),
id Varchar(32) PRIMARY KEY,
created_at varchar(50), 
asset_url Varchar(100), 
asset_hash Varchar(40) ,
"primary" Varchar(5)
)
diststyle ALL
;