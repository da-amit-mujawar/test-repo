--business occupations 
copy {table_business_occupations}
from 's3://{s3-cdbus-path}{s3-business-occupations-key11}'
iam_role '{iam}'
FORMAT AS PARQUET;










