drop table if exists mgen_ailments_flags CASCADE;
create table mgen_ailments_flags 
(
    cid varchar(18) DISTKEY SORTKEY, 
    ailments_flags varchar(500) ENCODE ZSTD
);

copy mgen_ailments_flags
from 's3://{s3-internal}/neptune/mGen/tblChild9_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_ailments_flags')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-ailments-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;