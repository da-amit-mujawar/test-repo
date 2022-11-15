UNLOAD ('SELECT * FROM tblBusinessIndividual_ctas1')
    TO 's3://idms-7933-prod/temp/business-csv/full'
IAM_ROLE 'arn:aws:iam::479134617933:role/da-idms-redshift-role'
FORMAT CSV
DELIMITER '|'
GZIP 
CLEANPATH
PARALLEL ON
MAXFILESIZE 200 MB;

