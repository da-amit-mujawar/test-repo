
/*
Reju Mathew 2018.12.05
*/


--FILE 1. Add ApplicationCodes

DROP TABLE IF EXISTS {tablename1};
CREATE TABLE {tablename1} (individual_id Varchar(21), cValue char(1));

COPY {tablename1}(
individual_id
)
FROM 's3://{s3-internal}{s3-key}' 
iam_role '{iam}'
 ;


CREATE TABLE {maintable_name};


