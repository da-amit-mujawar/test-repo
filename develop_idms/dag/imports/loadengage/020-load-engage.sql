--copy
copy {table-engaged}
from 's3://{s3-internal}/{s3-engaged-key20}/{table-engaged-input}'
iam_role '{iam}'
delimiter '|'
ignoreheader 1
;



