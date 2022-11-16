--load fixed-width file no header
copy {tablename1}
from 's3://{s3-internal}{s3-key1}{yyyy}{mm}{dd}'
iam_role '{iam}'
fixedwidth 'cphone:10'
;
