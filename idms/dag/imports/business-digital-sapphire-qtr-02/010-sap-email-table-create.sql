
--file after aop
drop table if exists {table_email_appended};

create table {table_email_appended}
(individualmc varchar(50),
 email varchar(100),
 listid varchar(100)
);

--create final email file
drop table if exists {table_sap_email_fnl};

create table {table_sap_email_fnl}
(individualmc varchar(50),
 email varchar(100),
 listid varchar(100)
);


