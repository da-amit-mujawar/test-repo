---Creating ehyg_dbid_Suppressions and load to S3
DROP TABLE IF EXISTS ehyg_{dbid}_Suppressions;
SELECT    ltrim(rtrim(email)) as Email 
INTO      ehyg_{dbid}_Suppressions 
FROM      ehyg_{dbid}_step2
WHERE     Ehygiene_Code = '0';
UNLOAD ('SELECT * FROM ehyg_{dbid}_Suppressions')
TO 's3://{unload_bucketname}/{suppresions_fkey}'
IAM_ROLE '{iam}'
ALLOWOVERWRITE
PARALLEL OFF
;

-- Creating ehyg_dbid_idms table AND unloading to S3
DROP TABLE IF EXISTS ehyg_{dbid}_IDMS;
SELECT    ltrim(rtrim(email)) as Email,
          Ehygiene_Code
INTO      ehyg_{dbid}_IDMS 
FROM      ehyg_{dbid}_step2
WHERE     Ehygiene_Code in ('1','2','3','4');
UNLOAD ('SELECT * FROM ehyg_{dbid}_IDMS')
TO 's3://{unload_bucketname}/{idms_fkey}'
IAM_ROLE '{iam}'
CSV DELIMITER AS '|'
ALLOWOVERWRITE
PARALLEL OFF
;