drop table if exists mgen_market_target_age_flags CASCADE;
create table mgen_market_target_age_flags 
(
    cid varchar(18) DISTKEY SORTKEY, 
    market_target_age_flags varchar(500) ENCODE ZSTD
);

copy mgen_market_target_age_flags
from 's3://{s3-internal}/neptune/mGen/tblChild12_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_market_target_age_flags')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-market-target-age-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;