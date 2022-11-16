-- select & split for email appendage, remove empty values

delete from {table_sap_main} where individualmc=' ';

unload ('select substring(individualmc, 1, 17) as individualmc,
    substring(company, 1, 50) as company,
    substring(firstname, 1, 20) as firstname,
    substring(lastname, 1, 20)  as lastname,
    substring(addr1, 1, 40)  as addr1,
    substring(addr2, 1, 40)  as addr2,
    substring(city, 1, 28)  as city,
    substring(st, 1, 2)  as st,
    substring(zip, 1, 5)  as zip,
    substring(zip4, 1, 4)  as zip4
    from {table_sap_main}')
    to 's3://{s3-aopinput}{table_sap_lr_email_append}'
iam_role '{iam}'
KMS_KEY_ID '{kmskey}'
encrypted
delimiter as '|'
allowoverwrite
--parallel off
--maxfilesize 3700 mb
gzip;

---to 's3://{s3-aopinput}{table_sap_lr_email_append}'
---to 's3://idms-2722-playground/ms{table_sap_lr_email_append}'
