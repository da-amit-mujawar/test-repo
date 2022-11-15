--APOGEE-163 Load B2CLink data to data lake
unload ('select * from  {maintable_name}')
to 's3://{s3-axle-gold}/b2clink/{yyyy}-{mm}/b2clink_'
iam_role '{iam}'
encrypted
allowoverwrite
parquet
manifest verbose;


DROP TABLE IF EXISTS {table-execureach-new-raw};
