COPY core_bf.{table}_changes
FROM 's3://{output_bucket}/{output_path}{table}_manifest.json'
IAM_ROLE '{iam}'
MANIFEST
IGNOREHEADER 1
IGNOREBLANKLINES 
TRIMBLANKS 
CSV
DELIMITER '|'
GZIP;