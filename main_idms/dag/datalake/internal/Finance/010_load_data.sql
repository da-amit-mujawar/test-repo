--Create staging table with all varchar type column
DROP TABLE IF EXISTS interna.finance_mothership_staging;
CREATE TABLE interna.finance_mothership_staging
(
    standard_name VARCHAR(65535),
    prod_detail VARCHAR(65535),
    prod_rollup_1 VARCHAR(65535),
    prod_rollup_2 VARCHAR(65535),
    prod_rollup_3 VARCHAR(65535),
    dig_team_flag VARCHAR(65535),
    old_bu VARCHAR(65535),
    bu VARCHAR(65535),
    sub_bu VARCHAR(65535),
    standard_division VARCHAR(65535),
    year VARCHAR(65535),
    digital_flag VARCHAR(65535),
    bu_nw VARCHAR(65535),
    sub_bu_nw VARCHAR(65535),
    div_nw  VARCHAR(65535),
    "asc" VARCHAR(65535),
    "version" VARCHAR(65535),
    old_division VARCHAR(65535),
    new_division VARCHAR(65535),
    client VARCHAR(65535),
    product_type VARCHAR(65535),
    month VARCHAR(65535),
    amount VARCHAR(65535),
    finversion VARCHAR(65535)
);

--Copy data from s3 to redshift staging table
COPY interna.finance_mothership_staging
FROM 's3://{bucket_name}/{file_key}'
IAM_ROLE '{iam}'
CSV QUOTE AS '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1;

--Create target table if not exist into redshift
CREATE TABLE IF NOT EXISTS interna.finance_mothership
(
    etl_id INT IDENTITY(1,1),
    standard_name VARCHAR(400),
    prod_detail VARCHAR(55),
    prod_rollup_1 VARCHAR(55),
    prod_rollup_2 VARCHAR(55),
    prod_rollup_3 VARCHAR(55),
    dig_team_flag VARCHAR(55),
    old_bu VARCHAR(55),
    bu VARCHAR(55),
    sub_bu VARCHAR(55),
    standard_division VARCHAR(55),
    year INT,
    digital_flag VARCHAR(55),
    bu_nw VARCHAR(55),
    sub_bu_nw VARCHAR(55),
    div_nw  VARCHAR(55),
    "asc" VARCHAR(55),
    "version" VARCHAR(55),
    old_division VARCHAR(55),
    new_division VARCHAR(55),
    client VARCHAR(400),
    product_type VARCHAR(55),
    month DATE,
    amount FLOAT,
    finversion VARCHAR(55)
);

INSERT INTO interna.finance_mothership
(
    standard_name,
    prod_detail,
    prod_rollup_1,
    prod_rollup_2,
    prod_rollup_3,
    dig_team_flag,
    old_bu,
    bu,
    sub_bu,
    standard_division,
    year,
    digital_flag,
    bu_nw,
    sub_bu_nw,
    div_nw,
    "asc",
    "version",
    old_division,
    new_division,
    client,
    product_type,
    month,
    amount,
    finversion
)
SELECT DISTINCT 
    standard_name,
    prod_detail,
    prod_rollup_1,
    prod_rollup_2,
    prod_rollup_3,
    dig_team_flag,
    old_bu,
    bu,
    sub_bu,
    standard_division,
    year::INT,
    digital_flag,
    bu_nw,
    sub_bu_nw,
    div_nw,
    "asc",
    "version",
    old_division,
    new_division,
    client,
    product_type,
    month::DATE,
    amount::FLOAT,
    finversion
FROM interna.finance_mothership_staging 
WHERE finversion
NOT IN 
    (SELECT DISTINCT finversion FROM interna.finance_mothership);

--Unload sample output
UNLOAD ('SELECT * FROM interna.finance_mothership WHERE etl_id 
		IN 
		(SELECT etl_id FROM interna.finance_mothership 
		WHERE finversion = (SELECT MAX(finversion) FROM interna.finance_mothership) LIMIT 100)')
TO 's3://{s3-internal}{reportname}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;