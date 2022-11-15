DROP TABLE IF EXISTS core_bf.images_new;

CREATE TABLE core_bf.images_new
(
infogroup_id varchar(15),
Images_id Varchar(32) PRIMARY KEY,
created_at varchar(50), 
asset_url Varchar(100), 
asset_hash Varchar(40) ,
"primary" Varchar(5)
)
diststyle ALL
;