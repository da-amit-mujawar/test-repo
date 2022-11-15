copy {tablename1} (LEMS,Deluxe_Segments)
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
delimiter '|'
;
