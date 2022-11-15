drop table if exists mgen_experian_child_array CASCADE;
create table mgen_experian_child_array 
(
    cid varchar(18) DISTKEY SORTKEY,
    experian_child_array varchar(500) ENCODE ZSTD
);

copy mgen_experian_child_array
from 's3://{s3-internal}/neptune/mGen/tblChild20_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_experian_child_array')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-experian-child-array/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;