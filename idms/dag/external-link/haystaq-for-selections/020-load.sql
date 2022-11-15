copy {tablename1} (Individual_ID,Haystaq_array)
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
IGNOREHEADER 1;
