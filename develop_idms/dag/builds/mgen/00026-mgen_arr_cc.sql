drop table if exists mgen_arr_cc CASCADE;
create table mgen_arr_cc 
(
    cid varchar(18) DISTKEY SORTKEY, 
    arr_cc varchar(500) ENCODE ZSTD
);

copy mgen_arr_cc
from 's3://{s3-internal}/neptune/mGen/tblChild30_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_arr_cc')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-arr-cc/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
