--create table for loading to redshift
drop table if exists {table-engaged};

create table {table-engaged} (
email              varchar(100) not null sortkey,
open_date          varchar(50) not null default '',
click_date         varchar(50) not null default '',
opnd               varchar(50),
clckd              varchar(50),
optout             varchar(50),
gst                varchar(1),
domain             varchar(80),
md5_upper_test     varchar(32),
md5_lower_test     varchar(32),
click_chg_flag     varchar(1)
)
diststyle all;



