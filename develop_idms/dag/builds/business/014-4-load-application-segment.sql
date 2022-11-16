DROP TABLE IF EXISTS ApplicationCodes_992;
CREATE TABLE ApplicationCodes_992 
(
  ABINumber varchar(9), 
  cValue varchar(1)
);


COPY ApplicationCodes_992  
(
ABINumber,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/application_992.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;

----------------------------------------------------------------------
DROP TABLE IF EXISTS SegmentCodes_992;
CREATE TABLE SegmentCodes_992 
(
ABINumber varchar(9), 
cValue varchar(1)
);


COPY SegmentCodes_992  
(
ABINumber ,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/segmentcode_992.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;

-----------------------------------------------------------------------
DROP TABLE IF EXISTS ApplicationCodes_992_E;
CREATE TABLE ApplicationCodes_992_E 
(
ABINumber varchar(9), 
ContactID varchar(12), 
cValue varchar(1)
);

COPY ApplicationCodes_992_E  
(
ABINumber,
ContactID,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/application_e_992.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;


-----------------------------------------------------------------------
DROP TABLE IF EXISTS SegmentCodes_992_E;
CREATE TABLE SegmentCodes_992_E 
(
ABINumber varchar(9), 
ContactID varchar(12), 
cValue varchar(1)
);

COPY SegmentCodes_992_E  
(
ABINumber,
ContactID,
cValue 
)
FROM 's3://idms-7933-internalfiles/business-core/segmentcode_e_992.csv'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1 FILLRECORD ;