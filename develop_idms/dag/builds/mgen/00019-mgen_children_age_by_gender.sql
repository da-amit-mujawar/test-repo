drop table if exists mgen_children_age_by_gender CASCADE;
create table mgen_children_age_by_gender 
(
    cid varchar(18) DISTKEY SORTKEY, 
    children_age_by_gender varchar(500) ENCODE ZSTD
);

copy mgen_children_age_by_gender
from 's3://{s3-internal}/neptune/mGen/tblChild18_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_children_age_by_gender')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-children-age-by-gender-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;