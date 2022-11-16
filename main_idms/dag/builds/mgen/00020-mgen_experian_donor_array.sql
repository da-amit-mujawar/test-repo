drop table if exists mgen_experian_donor_array CASCADE;
create table mgen_experian_donor_array 
(
    cid varchar(18) DISTKEY SORTKEY, 
    experian_donor_array varchar(500) ENCODE ZSTD
);

copy mgen_experian_donor_array
from 's3://{s3-internal}/neptune/mGen/tblChild19_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';


UNLOAD ('select * from mgen_experian_donor_array')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-experian-donor-array/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
