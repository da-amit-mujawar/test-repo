/*
New Meyer automation:
Refer to github.com/IDMS_IG/nua-airflow/Readme.md -- AIP processing

1. Extract a file for Previous DB (file code 100) (idms live build) in the below format with file name prefix as
    A06.filename to the following s3 bucket  -  s3://idms-7933-aop-input/IDMSAOPPI1/
            (AOP will trigger WB request 452038 with rule 5151)
2. Extract a file for Current DB (file code 200)  (idms new build) in the below format with file name prefix as
    A07.filename to the following s3 bucket  -  s3://idms-7933-aop-input/IDMSAOPPI1/
            (AOP will trigger WB request 441078 with rule 5171)
*/

unload ('select id,
            myrschoolid,
            myrrecordid,
            firstname,
            middleinitial,
            lastname,
            title,
            suffix,
            addressline1,
            ah1unitinfo,
            addressline2,
            city,
            state,
            zip,
            zip4,
            mcdindividualid,
            mcdhouseholdid
         from {pre_maintable_name};')
to 's3://{s3-aopinput}/A06.MEYER.PROMOHISTORY.INPUT.csv'
iam_role '{iam}'
csv delimiter as '|'
encrypted
parallel off
allowoverwrite
;

