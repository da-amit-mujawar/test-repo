--business main 
copy {table_business_main}
from 's3://{s3-cdbus-path}{s3-business-main-key11}'
iam_role '{iam}'
FORMAT AS PARQUET;












