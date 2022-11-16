drop table if exists mgen_pub2 CASCADE;

create table mgen_pub2
(
id int identity unique,  
cid varchar(18),
pub2_moslo int,
pub2_total_mag int,
pub2_total_pub_books int,
pub2_mag_moslo int,
pub2_mag_totord int,
pub2_active_mag int,
pub2_total_cat_books int,
pub2_mag_autornwl int,
pub2_expire_mag int,
pub2_mag_paid int,
pub2_mag_paid_cash int,
publisher2_source_arr varchar(12),
publisher2_cat1_arr varchar(80),
publisher2_cat2_arr varchar(50)
)
DISTSTYLE KEY
DISTKEY(cid)
SORTKEY(cid);

copy mgen_pub2
from 's3://{s3-internal}/neptune/mGen/pub2_data.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_pub2')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-pub2/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;

