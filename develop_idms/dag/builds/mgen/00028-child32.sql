drop table if exists tblchild32_{build_id}_{build} CASCADE;
create table tblchild32_{build_id}_{build} 
(
    cid varchar(18) DISTKEY SORTKEY, 
    cvalue varchar(1500) ENCODE ZSTD
);


copy tblchild32_{build_id}_{build}
from 's3://{s3-internal}/neptune/mGen/tblChild24_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from tblchild32_{build_id}_{build}')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-arr-12m/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;