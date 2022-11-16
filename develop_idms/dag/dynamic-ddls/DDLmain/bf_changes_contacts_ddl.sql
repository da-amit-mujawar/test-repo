DROP TABLE IF EXISTS core_bf.contacts;

CREATE TABLE core_bf.contacts
( 
infogroup_id varchar(15) ,
contact_id Varchar(32) primary key,
created_at varchar(50) ,
email Varchar(80) ,
email_md5 Varchar(32) ,
email_sha256 Varchar(64) ,
email_vendor_id varchar(10) ,
email_deliverable varchar(5) ,
email_marketable varchar(5) ,
email_reputation_risk varchar(6) ,
labels_email_reputation_risk varchar(6) ,
first_name varchar(60) ,
gender varchar(1) ,
labels_gender varchar(6) ,
job_function_id int ,
labels_job_function_id Varchar(100) ,  
job_titles varchar(1000) ,
job_titles_count int ,
job_title_ids varchar(1000) ,
job_title_ids_count int ,
last_name varchar(60) ,
management_level varchar(30) ,
labels_management_level varchar(30) ,
mapped_contact_id varchar(15) ,
professional_title varchar(50) ,
labels_professional_title Varchar(80) ,
"primary" varchar(5) ,
suppressed_fields varchar(1000) ,
suppressed_fields_count int ,
title_codes varchar(1000) ,
labels_title_codes Varchar(1000) ,
title_codes_count int ,
vendor_id varchar(10) 
)
diststyle ALL
;