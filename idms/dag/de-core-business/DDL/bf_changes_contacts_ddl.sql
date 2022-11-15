DROP TABLE IF EXISTS core_bf.contacts_changes;

CREATE TABLE core_bf.contacts_changes
( 
infogroup_id varchar(15) ENCODE ZSTD ,
id Varchar(32) primary key ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD ,
email Varchar(80) ENCODE ZSTD ,
email_md5 Varchar(32) ENCODE ZSTD ,
email_sha256 Varchar(64) ENCODE ZSTD ,
email_vendor_id varchar(10) ENCODE ZSTD ,
email_deliverable varchar(5)  ENCODE ZSTD,
email_marketable varchar(5)  ENCODE ZSTD,
email_reputation_risk varchar(6)  ENCODE ZSTD,
labels_email_reputation_risk varchar(6)  ENCODE ZSTD,
first_name varchar(60)  ENCODE ZSTD,
gender varchar(1)  ENCODE ZSTD,
labels_gender varchar(6)  ENCODE ZSTD,
job_function_id int  ENCODE ZSTD,
labels_job_function_id Varchar(100) ENCODE ZSTD ,
job_titles varchar(1000) ENCODE ZSTD ,
job_titles_count int ENCODE ZSTD ,
job_title_ids varchar(1000) ENCODE ZSTD ,
job_title_ids_count int ENCODE ZSTD ,
last_name varchar(60) ENCODE ZSTD ,
management_level varchar(30) ENCODE ZSTD ,
labels_management_level varchar(30) ENCODE ZSTD ,
mapped_contact_id varchar(15) ENCODE ZSTD ,
professional_title varchar(50) ENCODE ZSTD ,
labels_professional_title Varchar(80) ENCODE ZSTD ,
"primary" varchar(5) ENCODE ZSTD ,
suppressed_fields varchar(1000) ENCODE ZSTD ,
suppressed_fields_count int ENCODE AZ64 ,
title_codes varchar(1000) ENCODE ZSTD ,
labels_title_codes Varchar(1000) ENCODE ZSTD ,
title_codes_count int ENCODE ZSTD ,
vendor_id varchar(10) ENCODE ZSTD
)
diststyle ALL
;