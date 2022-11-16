DROP TABLE IF EXISTS ehyg_{dbid};
CREATE TABLE ehyg_{dbid}
(
    email VARCHAR(100)
);

--Copy data from s3 to redshift table
COPY ehyg_{dbid}
FROM 's3://{bucket_name}/{file_key}'
IAM_ROLE '{iam}'
CSV QUOTE AS '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1;

UPDATE ehyg_{dbid}
SET email = replace(replace(email,'"',''),',','')
WHERE nvl(email,'') != '';

-- send file to oversight via aop
UNLOAD ('select distinct(rtrim(ltrim(email))) as email 
        from ehyg_{dbid} where nvl(email, '''') != ''''')
TO 's3://{s3-aopinput}/{emailoversight-prefix}_d045000_{dbid}_{yyyy}{mm}{dd}_'
-- TO 's3://idms-2722-playground/mmk/email-hygiene-unload/{emailoversight-prefix}_d045000_{dbid}_{yyyy}{mm}{dd}_'
IAM_ROLE '{iam}'
ALLOWOVERWRITE
PARALLEL OFF
;

--Unload sample output
UNLOAD ('select count(distinct email) from  ehyg_{dbid} 
        where nvl(email, '''') != ''''')
TO 's3://{s3-internal}{reportname}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;
