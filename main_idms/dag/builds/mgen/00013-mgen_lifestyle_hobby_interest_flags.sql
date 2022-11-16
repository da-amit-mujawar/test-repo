drop table if exists mgen_lifestyle_hobby_interest_flags CASCADE;
create table mgen_lifestyle_hobby_interest_flags 
(
    cid varchar(18) DISTKEY SORTKEY, 
    lifestyle_hobby_interest_flags varchar(500) ENCODE ZSTD
);

copy mgen_lifestyle_hobby_interest_flags
from 's3://{s3-internal}/neptune/mGen/tblChild8_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_lifestyle_hobby_interest_flags')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-lifestyle-hobby-interest-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;