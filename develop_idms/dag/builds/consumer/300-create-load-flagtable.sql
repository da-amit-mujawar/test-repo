DROP TABLE IF EXISTS nosuchtable;
--fulfillment flag
DROP TABLE IF EXISTS CONSUMER_1267_TEMPSUPPRESS_ToBeDropped;
CREATE TABLE CONSUMER_1267_TEMPSUPPRESS_ToBeDropped 
(
    individual_id Varchar(21), 
    cValue char(1)
);

COPY CONSUMER_1267_TEMPSUPPRESS_ToBeDropped
(
individual_id
)
FROM 's3://{s3-internal}/idms-mmdb/consumer/dailysuppress.csv' 
iam_role '{iam}';

--donotcall flag
DROP TABLE IF EXISTS CONSUMER_1267_TEMPFILE_ToBeDropped;
CREATE TABLE CONSUMER_1267_TEMPFILE_ToBeDropped 
(company_id varchar(20), 
cValue char(1), 
company_id_int bigint);

COPY CONSUMER_1267_TEMPFILE_ToBeDropped(
company_id,
cValue
)
FROM 's3://{s3-internal}/consumer-core/donotcallflag.dat' 
iam_role '{iam}'
IGNOREHEADER as 1
delimiter ','
removequotes
 ;

update CONSUMER_1267_TEMPFILE_ToBeDropped
set company_id_int = cast(company_id as bigint);