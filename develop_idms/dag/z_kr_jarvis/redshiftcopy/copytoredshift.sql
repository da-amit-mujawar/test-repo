COPY Sample_Business_Changes_TBL
FROM 's3://idms-2722-internalfiles/transfomer_dev/output/parquets/part-00000-tid-6724967054207306917-b74f335a-f8d7-4829-82cc-5da09bc7236b-7-1-c000.snappy.parquet'
IAM_ROLE 'arn:aws:iam::250245842722:role/da-idms-redshift-role'
kms_key_id '{kmskey}'
FORMAT AS PARQUET;