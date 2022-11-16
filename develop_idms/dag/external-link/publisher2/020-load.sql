copy {tablename1}
(
LEMS,
Pub2_MS_LMOS,
Pub2_MS_MAGS,
Pub2_MS_BOOKS,
Pub2_MS_PUB_LMOS,
Pub2_MS_PUB_TOTORDS,
Pub2_MS_PUB_STATUS_ACTIVE,
Pub2_PKCA42,
Pub2_MS_PUB_AUTORNWL,
Pub2_MS_PUB_STATUS_EXPIRE,
Pub2_MS_PUB_PAYSTAT_PAID,
Pub2_MS_PUB_PAYMETH_CASH,
Pub2_source_arr,
Pub2_cat1_arr,
Pub2_cat2_arr,
PUB2SOURCE
)
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
;
