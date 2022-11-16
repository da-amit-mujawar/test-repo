drop table if exists mgen_children_month_of_birth CASCADE;
create table mgen_children_month_of_birth 
(
    cid varchar(18) DISTKEY SORTKEY, 
    children_month_of_birth varchar(500) ENCODE ZSTD
);

copy mgen_children_month_of_birth
from 's3://{s3-internal}/neptune/mGen/tblChild17_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_children_month_of_birth')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-children-month-of-birth-flags/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;