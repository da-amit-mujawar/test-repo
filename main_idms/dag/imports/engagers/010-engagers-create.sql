-- sample record
    -- email,open_date,click_date,opnd,clckd,optout
    -- #6@ATTGLOBAL.NET,20080722,20080722,1,0,

--create table for loading to redshift
drop table if exists {tablename1};

create table {tablename1}(
    EmailAddress varchar(200) not null default '' sortkey,
    OpenDate VARCHAR(20) not null default '',
    ClickDate VARCHAR(20) not null default '',
    Filler_Opened VARCHAR(20) not null default '',
    Filler_Clicked VARCHAR(20) not null default '',
    Filler_Optout VARCHAR(20) not null default ''
 )
diststyle even;

--create table for distinct email and Last Click Open date
drop table if exists {tabledistinct1};

create table {tabledistinct1}(
    EmailAddress varchar(65) not null sortkey,
    OpenClickDate VARCHAR(6) not null default ''
 )
diststyle even;

