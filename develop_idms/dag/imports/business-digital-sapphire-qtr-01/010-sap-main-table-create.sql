--create stat table
drop table if exists {table_job_stats};

create table {table_job_stats}
(task varchar(150),quantity bigint, run_date timestamp sortkey);

--create main table
drop table if exists {table_sap_main};

create table {table_sap_main}
(companymc varchar(100),
firstname varchar(100),
fullname varchar(300),
individualmc varchar(100),
lastname varchar(100),
addr1 varchar(300),
addr2 varchar(300),
address_type varchar(100),
city varchar(100),
company varchar(300),
country_name varchar(100),
gender varchar(100),
contact_phone varchar(100),
st varchar(100),
title varchar(100),
zip varchar(100),
zip4 varchar(100),
address_type_indicator varchar(100),
dma_do_not_call varchar(100),
dma_mail_preference varchar(100),
record_type varchar(100),
resbus_indicator varchar(100),
text_job_function varchar(100),
text_job_title varchar(100),
employees_combined varchar(100),
infousa_year_established varchar(100),
multi_buyer_count varchar(100),
sic2 varchar(100),
sic4 varchar(100),
text_title_function_one_click varchar(100),
industry_p varchar(100),
sales_volume varchar(100),
text_jobfunction varchar(100),
text_jobtitle varchar(100),
listid varchar(100),
deliverypoint varchar(100),
deliverypointdropind varchar(100),
daus_abinum varchar(100),
filler1 varchar(1),
id bigint
);






