--load pipe delimited text file with header
copy {tablename1}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
ignoreheader 1;
