
--Location_Type
drop table if exists {table_location_type_segment};

create table {table_location_type_segment}
(individualmc varchar(100),
location_type varchar(1000))
sortkey (individualmc);

--Interest_Area
drop table if exists {table_interest_area_segment};

create table {table_interest_area_segment}
(individualmc varchar(100),
interest_area varchar(1000))
sortkey (individualmc);

--Product_Specified
drop table if exists {table_product_specified_segment};

create table {table_product_specified_segment}
(individualmc varchar(100),
product_specified varchar(1000))
sortkey (individualmc);

--Job_function
drop table if exists {table_job_function_segment};

create table {table_job_function_segment}
(individualmc varchar(100),
job_function varchar(1000))
sortkey (individualmc);

--INDUSTRY_M
drop table if exists {table_industry_m_segment};

create table {table_industry_m_segment}
(individualmc varchar(100),
industry_m varchar(1000))
sortkey (individualmc);

--Technology_On_Site_h
drop table if exists {table_technology_on_site_h_segment};

create table {table_technology_on_site_h_segment}
(individualmc varchar(100),
technology_on_site_h varchar(1000))
sortkey (individualmc);

--Technology_On_Site_s
drop table if exists {table_technology_on_site_s_segment};

create table {table_technology_on_site_s_segment}
(individualmc varchar(100),
technology_on_site_s varchar(1000))
sortkey (individualmc);

--create bluekai
drop table if exists {table_sap_bluekai};

create table {table_sap_bluekai}
(individualmc varchar(100),listid varchar(100),daus_abinum varchar(100),
SP001 varchar(1),SP002 varchar(1),SP031 varchar(1),SP040 varchar(1),SP041 varchar(1),SP043 varchar(1),
SP045 varchar(1),SP125 varchar(1),SP134 varchar(1),SP136 varchar(1),SP143 varchar(1),SP144 varchar(1),
SP146 varchar(1),SP147 varchar(1),SP149 varchar(1),SP237 varchar(1),SP239 varchar(1),SP241 varchar(1),
SP242 varchar(1),SP243 varchar(1),SP245 varchar(1),SP247 varchar(1),SP248 varchar(1),SP249 varchar(1),
SP250 varchar(1),SP251 varchar(1),SP252 varchar(1),SP253 varchar(1),SP254 varchar(1),SP256 varchar(1),
SP257 varchar(1),SP259 varchar(1),SP261 varchar(1),SP030 varchar(1),SP036 varchar(1),SP037 varchar(1),
SP047 varchar(1),SP049 varchar(1),SP050 varchar(1),SP051 varchar(1),SP052 varchar(1),SP054 varchar(1),
SP055 varchar(1),SP056 varchar(1),SP057 varchar(1),SP062 varchar(1),SP060 varchar(1),SP063 varchar(1),
SP066 varchar(1),SP067 varchar(1),SP068 varchar(1),SP129 varchar(1),SP138 varchar(1),SP142 varchar(1),
SP069 varchar(1),SP070 varchar(1),SP061 varchar(1),SP065 varchar(1),SP277 varchar(1),SP276 varchar(1)
);
