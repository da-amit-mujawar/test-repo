drop table if exists mgen_interest_flag CASCADE;
create table mgen_interest_flag 
(
    cid varchar(18) DISTKEY SORTKEY, 
    interest_flag varchar(500) ENCODE ZSTD
);

copy mgen_interest_flag
from 's3://{s3-internal}/neptune/mGen/tblChild7_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_interest_flag')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-interest-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
