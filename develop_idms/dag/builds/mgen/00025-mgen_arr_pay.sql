drop table if exists mgen_arr_pay CASCADE;
create table mgen_arr_pay 
(
    cid varchar(18) DISTKEY SORTKEY, 
    arr_pay varchar(500) ENCODE ZSTD
);

copy mgen_arr_pay
from 's3://{s3-internal}/neptune/mGen/tblChild29_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_arr_pay')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-arr-pay/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;