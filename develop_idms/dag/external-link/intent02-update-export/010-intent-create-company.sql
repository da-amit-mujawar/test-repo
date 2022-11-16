--create company table for loading to redshift
insert into {table_job_stats} values
('Start Intent Monthly Load after AOP',0,getdate() );


drop table if exists {table_company};

create table {table_company}
(
account_link_id varchar(100) PRIMARY KEY,
company varchar(150),
address varchar(100),
city varchar(30),
st varchar(2),
zip varchar(10),
AH1_DPV_CONFIRM_INDC varchar(1),
AH1_VACANT_INDC varchar(1),
AH1_SEASONAL_INDC varchar(1),
AH1_RES_BUS_INDC varchar(1),
AH1_DELIVERY_TYPE_CODE varchar(1),
AH1_LOCAL_ADDRESS varchar(40),
AH1_UNIT_INFORMATION varchar(13),
AH1_SECONDARY_ADDRESS varchar(40),
AH1_LONG_CITY_NAME varchar(28),
AH1_STATE_ABBREVIATION varchar(2),
AH1_ZIP_CODE varchar(5),
AH1_ZIP4_CODE varchar(4),
AH1_MAILABILITY_SCORE varchar(2),
AH1_CARRIER_ROUTE varchar(4),
AH1_LOT_NUMBER varchar(5),
AH1_LOT_SORTATION_NUMBER varchar(2),
AH1_DELIVERY_POINT1 varchar(3),
AH1_MATCH_CODE varchar(18),
IDMS_cMatchCode varchar(15)
);

