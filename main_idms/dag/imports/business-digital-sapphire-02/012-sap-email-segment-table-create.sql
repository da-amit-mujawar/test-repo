
--create Sapphire email segmented table
drop table if exists {table_sap_email_segmented};

create table {table_sap_email_segmented}
(individualmc varchar(50),
email varchar(100),
listid varchar(50),
SP001 varchar(1),SP002 varchar(1),SP031 varchar(1),SP040 varchar(1),SP041 varchar(1),SP043 varchar(1),
SP045 varchar(1),SP125 varchar(1),SP134 varchar(1),SP143 varchar(1),
SP146 varchar(1),SP147 varchar(1),SP149 varchar(1),SP237 varchar(1),SP239 varchar(1),SP241 varchar(1),
SP242 varchar(1),SP243 varchar(1),SP245 varchar(1),SP247 varchar(1),SP248 varchar(1),SP249 varchar(1),
SP250 varchar(1),SP251 varchar(1),SP252 varchar(1),SP253 varchar(1),SP254 varchar(1),SP256 varchar(1),
SP257 varchar(1),SP259 varchar(1),SP261 varchar(1),SP030 varchar(1),SP036 varchar(1),SP037 varchar(1),
SP047 varchar(1),SP049 varchar(1),SP050 varchar(1),SP051 varchar(1),SP052 varchar(1),SP054 varchar(1),
SP055 varchar(1),SP056 varchar(1),SP057 varchar(1),SP062 varchar(1),SP060 varchar(1),SP063 varchar(1),
SP066 varchar(1),SP067 varchar(1),SP068 varchar(1),SP129 varchar(1),SP138 varchar(1),SP142 varchar(1),
SP069 varchar(1),SP070 varchar(1),SP061 varchar(1),SP277 varchar(1),SP276 varchar(1),
row_no varchar(10)
);