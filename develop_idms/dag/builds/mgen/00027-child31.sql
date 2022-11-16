drop table if exists tblchild31_{build_id}_{build} CASCADE;
create table tblchild31_{build_id}_{build} 
(
    cid varchar(18) DISTKEY SORTKEY, 
    cvalue varchar(1500) ENCODE ZSTD
);

copy tblchild31_{build_id}_{build}
from 's3://{s3-internal}/neptune/mGen/tblChild23_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from tblchild31_{build_id}_{build}')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-arr-3m/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;