

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

insert into {table_sap_main} (
companymc ,
firstname ,
fullname ,
individualmc ,
lastname ,
addr1 ,
addr2 ,
address_type ,
city ,
company ,
country_name ,
gender ,
contact_phone ,
st ,
title ,
zip ,
zip4 ,
address_type_indicator ,
dma_do_not_call ,
dma_mail_preference ,
record_type ,
resbus_indicator ,
text_job_function ,
text_job_title ,
employees_combined ,
infousa_year_established ,
multi_buyer_count ,
sic2 ,
sic4 ,
text_title_function_one_click ,
industry_p ,
sales_volume ,
text_jobfunction ,
text_jobtitle ,
listid ,
deliverypoint ,
deliverypointdropind ,
daus_abinum ,
id
)
SELECT MainTable.Company_MC,MainTable.FirstName,MainTable.FullName,MainTable.Individual_ID,MainTable.LastName,MainTable.AddressLine1,MainTable.AddressLine2,
MainTable.AddressType,MainTable.City,MainTable.Company,MainTable.COUNTRYNAME,MainTable.Gender,LEFT(CAST(Phone AS varchar(30)),50),MainTable.State,MainTable.Title,
MainTable.ZIP,MainTable.ZIP4,MainTable.AddressTypeIndicator,MainTable.DMAPhoneSuppress,MainTable.DMAMailPreference,MainTable.RecordType,MainTable.ResBusIndicator,
MainTable.Text_JobFunction1,MainTable.Text_JobTitle,MainTable.EmployeesCombined_Prioritized,MainTable.INFO_YRESTBLSHD,TRIM(CAST(MainTable.MultiBuyerCount as VARCHAR(50))),
MainTable.SIC2_Prioritized,MainTable.SIC4_Prioritized,MainTable.CombinedJobTitleAndFunction_rollup,MainTable.Industry_Prioritized,MainTable.SalesVolume_Prioritized,
MainTable.Text_JobFunction_Rollup,MainTable.Text_JobTitle_Rollup,TRIM(CAST(MainTable.ListID as VARCHAR(50))),MainTable.DeliveryPoint,MainTable.DeliveryPointDropInd,
MainTable.IGUS_ABINUM, CountTable.DWID
FROM {maintable_name} as MainTable  INNER JOIN COUNT_1464634 as CountTable ON MainTable.ID=CountTable.DWID  AND CountTable.Individual_ID!='';

insert into {table_job_stats}
select 'Load Sap Main File Table',count(*),getdate() from {table_sap_main};

delete from {table_sap_main} where zip is NULL;

insert into {table_job_stats}
select 'Drop NULL Zipcodes from Sap Main File Table',count(*),getdate() from {table_sap_main};

delete from {table_sap_main}
where substring(IndividualMC,1,1) ~ '[^[:digit:]]';

insert into {table_job_stats}
select 'Drop Bad IndividualMC from Sap Main File Table',count(*),getdate() from {table_sap_main};

