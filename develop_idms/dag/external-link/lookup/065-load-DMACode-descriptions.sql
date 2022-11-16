/*
#783178  992 - Add DMA Code Description to **US Business ACL AMI TESTING (17386) build 
Reju Mathew 2019.06.12
*/




DROP TABLE IF EXISTS  {tablename12};

CREATE TABLE  {tablename12}(
DMACode VARCHAR(5) SORTKEY PRIMARY KEY,
Descriptions VARCHAR(100)
)  ; 


copy {tablename12}
from 's3://{s3-internal}{s3-key1}' 
iam_role '{iam}'
delimiter ','
IGNOREHEADER 1 ;   


/*
Count matched
sample data matched
*/

