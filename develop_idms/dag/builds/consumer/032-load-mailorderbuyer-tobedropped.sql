DROP TABLE IF EXISTS NoSuchTable;






DROP TABLE IF EXISTS {MailOrderBuyer_ToBeDropped-load-table};
CREATE TABLE {MailOrderBuyer_ToBeDropped-load-table}
(

    familyid VARCHAR(12),
    ccode VARCHAR(2)

);

COPY {MailOrderBuyer_ToBeDropped-load-table}
(

    familyid,
    ccode
)

FROM 's3://{s3-internal}{s3-key-MailOrderBuyer}'
IAM_ROLE '{iam}'
IGNOREHEADER AS 1
DELIMITER ','
REMOVEQUOTES;