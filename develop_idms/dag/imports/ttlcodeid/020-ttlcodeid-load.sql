--load fixed width file no header
copy {tablename1}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
fixedwidth 'titleid:8,titledescription:256'
;

