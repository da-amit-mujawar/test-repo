--business sic 
copy {table_business_sic}
from 's3://{s3-cdbus-path}{s3-business-sic-key11}'
iam_role '{iam}'
FORMAT AS PARQUET;










