DROP TABLE IF EXISTS NoSuchTable;



DROP TABLE IF EXISTS {segcode-load-table};

CREATE TABLE {segcode-load-table} 
(
    individual_id VARCHAR(20), 
    cvalue VARCHAR(1)
);

COPY {segcode-load-table}
(
    individual_id,
    cvalue
)
FROM 's3://{s3-internal}{s3-key-segcode}' 
IAM_ROLE '{iam}'
IGNOREHEADER AS 1
DELIMITER ','
REMOVEQUOTES
 ;


