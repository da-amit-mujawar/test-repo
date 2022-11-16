--this is for creating files for aop to pickup
--they want file to be breakup into 10 mil records per file
--Initial process should contain about 280 mil unique individuals
--use maxfilesize of 500 mb will give us about 7 mil records per file with 42 files total

UNLOAD ('SELECT max(id) id, personal_name, primary_address, secondary_address, city, state, zip, zip_4
           FROM {mailfile_table} 
          WHERE aop_date is null and personal_name <> ''''
          GROUP BY personal_name, primary_address, secondary_address, city, state, zip, zip_4')
    TO 's3://{s3-aopinput}/a03_apgpromo_'
-- to 's3://idms-2722-internalfiles/Reports/apogee_promo_aop/a03_apgpromo_'  --use this for testing only
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
HEADER
MAXFILESIZE 500 mb
CLEANPATH
;
