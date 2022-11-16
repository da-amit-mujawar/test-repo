DROP TABLE IF EXISTS temp_donorbase;
create table temp_donorbase
(
    order_id int,
    IDMS_iMatchCode varchar(17),
    IDMS_cMatchCode varchar(15)
);

copy temp_donorbase
FROM 's3://{bucket_name}/{file_key}'
IAM_ROLE '{iam}'
ACCEPTINVCHARS
GZIP
IGNOREHEADER 1;