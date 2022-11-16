DROP TABLE IF EXISTS ApplicationCodes_1293;
CREATE TABLE ApplicationCodes_1293 
(
  ABINumber varchar(9), 
  cValue varchar(1)
);


COPY ApplicationCodes_1293  
(
ABINumber,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/application_1293.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;

----------------------------------------------------------------------
DROP TABLE IF EXISTS SegmentCodes_1293;
CREATE TABLE SegmentCodes_1293 
(
ABINumber varchar(9), 
cValue varchar(1)
);


COPY SegmentCodes_1293  
(
ABINumber ,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/segmentcode_1293.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;

-----------------------------------------------------------------------
DROP TABLE IF EXISTS ApplicationCodes_1293_E;
CREATE TABLE ApplicationCodes_1293_E 
(
ABINumber varchar(9), 
ContactID varchar(12), 
cValue varchar(1)
);

COPY ApplicationCodes_1293_E  
(
ABINumber,
ContactID,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/application_e_1293.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;


-----------------------------------------------------------------------
DROP TABLE IF EXISTS SegmentCodes_1293_E;
CREATE TABLE SegmentCodes_1293_E 
(
ABINumber varchar(9), 
ContactID varchar(12), 
cValue varchar(1)
);

COPY SegmentCodes_1293_E  
(
ABINumber,
ContactID,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/segmentcode_e_1293.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;