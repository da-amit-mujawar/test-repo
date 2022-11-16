drop table if exists mgen_arr_twm CASCADE;
create table mgen_arr_twm 
(
    cid varchar(18) DISTKEY SORTKEY, 
    arr_twm varchar(500) ENCODE ZSTD
);

copy mgen_arr_twm
from 's3://{s3-internal}/neptune/mGen/tblChild22_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_arr_twm')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-arr-twm/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
