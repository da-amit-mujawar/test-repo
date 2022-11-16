drop table if exists mgen_credit_card_flag CASCADE;
create table mgen_credit_card_flag 
(
    cid varchar(18) DISTKEY SORTKEY, 
    credit_card_flag varchar(500) ENCODE ZSTD
);

copy mgen_credit_card_flag
from 's3://{s3-internal}/neptune/mGen/tblChild10_1196_201301_RU.txt'
iam_role '{iam}'
delimiter '|';

UNLOAD ('select * from mgen_credit_card_flag')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-credit-card-flag/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;