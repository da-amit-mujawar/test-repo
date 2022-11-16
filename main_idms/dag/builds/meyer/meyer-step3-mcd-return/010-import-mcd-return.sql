drop table if exists meyer_mcd_return;

create table meyer_mcd_return (
    sequence varchar(10),
    channel	varchar(3),
    file_no varchar(3),
    enterpriseid varchar(20),   --mcdindividualid
    collection varchar(20),     --mcdhouseholdid
    title	varchar(30),
    first	varchar(30),
    middle	varchar(30),
    last	varchar(30),
    generational	varchar(30),
    gender	varchar(1),
    dob	varchar(8),
    address	varchar(100),
    secondary	varchar(100),
    unit	varchar(10),
    city	varchar(50),
    province	varchar(30),
    postal	varchar(6),
    plus4	varchar(4),
    account	varchar(100),
    phone	varchar(20),
    email	varchar(75),
    company	varchar(100),
    division	varchar(30),
    since	varchar(30),
    aq	varchar(2),
    f_sequence	varchar(10),
    f_file	varchar(3),
    f_run	varchar(10),
    p_id	varchar(20),
    p_collection	varchar(20),
    extra01	varchar(100),
    extra02	varchar(100),
    extra03	varchar(100),
    extra04	varchar(100),
    extra05	varchar(100),
    extra06	varchar(100),
    extra07	varchar(100),
    extra08	varchar(100),
    extra09	varchar(100),
    extra10	varchar(100),
    extra11	varchar(100),
    extra12	varchar(100),
    extra13	varchar(100),
    extra14	varchar(100),
    extra15	varchar(100),
    extra16	varchar(100),
    extra17	varchar(100),
    extra18	varchar(100),
    extra19	varchar(100),
    extra20	varchar(100),
    extra21	varchar(100),
    extra22	varchar(100),
    extra23	varchar(100),
    extra24	varchar(100),
    extra25	varchar(100),
    extra26	varchar(100),
    extra27	varchar(100),
    extra28	varchar(100),
    extra29	varchar(100),
    extra30	varchar(100),
    extra31	varchar(100),
    extra32	varchar(100)
);


copy meyer_mcd_return
from 's3://idms-7933-aop-output/a05_meyers_work_matched.dat'
iam_role '{iam}'
delimiter ';'
acceptinvchars
;

