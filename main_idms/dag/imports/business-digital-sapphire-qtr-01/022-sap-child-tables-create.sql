
--create child tables

drop table if exists {table_sap_hardware};   
                                          
create table {table_sap_hardware}            
(individualmc varchar(100),
technology_on_site_h varchar(1000),
filler1 varchar(1),
id bigint                                  
);  

drop table if exists {table_sap_software};

create table {table_sap_software}
(individualmc varchar (100),
technology_on_site_s varchar (1000),
filler1 varchar(1),
id bigint
);

drop table if exists {table_sap_industry};

create table {table_sap_industry}
(individualmc varchar (100),
industry_m varchar (1000),
filler1 varchar(1),
id bigint
);


drop table if exists {table_sap_interestarea};

create table {table_sap_interestarea}
(individualmc varchar (100),
interest_area varchar (1000),
filler1 varchar(1),
id bigint
);


drop table if exists {table_sap_jobfunction};

create table {table_sap_jobfunction}
(individualmc varchar (100),
job_function varchar (1000),
filler1 varchar(1),
id bigint
);

drop table if exists {table_sap_locationtype};

create table {table_sap_locationtype}
(individualmc varchar (100),
location_type varchar (1000),
filler1 varchar(1),
id bigint
);

drop table if exists {table_sap_productspecified};

create table {table_sap_productspecified}
(individualmc varchar (100),
product_specified varchar (1000),
filler1 varchar(1),
id bigint
);

drop table if exists {table_sap_specialty};

create table {table_sap_specialty}
(individualmc varchar (100),
specialty varchar (1000),
filler1 varchar(1),
id bigint
);

