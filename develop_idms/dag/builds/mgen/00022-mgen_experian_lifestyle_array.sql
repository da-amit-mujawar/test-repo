drop table if exists mgen_experian_lifestyle_array CASCADE;
create table mgen_experian_lifestyle_array 
(
    cid varchar(18) DISTKEY SORTKEY, 
    experian_lifestyle_array varchar(500) ENCODE ZSTD
);

copy mgen_experian_lifestyle_array
from 's3://{s3-internal}/neptune/mGen/tblChild21_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_experian_lifestyle_array')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-experian-lifestyle-array/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
