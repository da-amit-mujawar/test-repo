drop table if exists mgen_dlx_segments CASCADE;
create table mgen_dlx_segments 
(
    cid varchar(18) DISTKEY SORTKEY, 
    dlx_segments varchar(500) ENCODE ZSTD
);

copy mgen_dlx_segments
from 's3://{s3-internal}/neptune/mGen/tblChild26_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_dlx_segments')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-dlx-segments/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
