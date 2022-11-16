drop table if exists mgen_donor_flags CASCADE;
create table mgen_donor_flags 
(
    cid varchar(18) DISTKEY SORTKEY, 
    donor_flags varchar(500) ENCODE ZSTD
);

copy mgen_donor_flags
from 's3://{s3-internal}/neptune/mGen/tblChild11_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_donor_flags')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-donor-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;