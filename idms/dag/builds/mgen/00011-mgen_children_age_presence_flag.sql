drop table if exists mgen_children_age_presence_flag CASCADE;
create table mgen_children_age_presence_flag 
(
    cid varchar(18) DISTKEY SORTKEY, 
    children_age_presence_flag varchar(500) ENCODE ZSTD
);

copy mgen_children_age_presence_flag
from 's3://{s3-internal}/neptune/mGen/tblChild40_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_children_age_presence_flag')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-children-age-presence-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;