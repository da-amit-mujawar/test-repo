DROP TABLE IF EXISTS NoSuchTable;



DROP TABLE IF EXISTS {PreferredCity-load-table}  ;
CREATE TABLE {PreferredCity-load-table}
(

    filler1 VARCHAR(11),
    zipcode VARCHAR(5) UNIQUE,
    city VARCHAR(30),
    filler2 VARCHAR(9)

);

COPY {PreferredCity-load-table}
FROM 's3://{s3-internal}{s3-key-PreferredCity_Load}'
IAM_ROLE '{iam}'
FIXEDWIDTH
'filler1 :11,
zipcode :5,
city :30,
filler2 :9' ;


