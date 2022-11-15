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
            myrtitle,
            myrfirstname,
            myrmiddlename,
            myrlastname,
            myrsuffix,
            myrgender,
            myrdob,
            myraddrline1,
            myraddrline2,
            myraddrline3,
            myraddrline4,
            myraddrline5,
            myrcity,
            myrstate,
            myrzip,
            cemail,
            myrspousetitle,
            myrspousefirstname,
            myrspousemiddlename,
            myrspouselastname,
            myrspousesuffix,
            raw_field_spouse_gender,
            myrspousedobccyymmdd,
            myrschoolparsefullname,
            myrschoolparsespousefullname
         from {maintable_name};')
to 's3://{s3-aopinput}/A07.MEYER.CURRENT.INPUT.csv'
iam_role 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
csv delimiter as '|'
encrypted
parallel off
allowoverwrite
;
