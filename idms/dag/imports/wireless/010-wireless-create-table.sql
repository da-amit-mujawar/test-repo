--create table for loading to redshift
drop table if exists {wireless-tablename1};

CREATE TABLE {wireless-tablename1}
(
CellPhone_Areacode varchar(3),
CellPhone_Number varchar(7),
Firstname varchar(50),
Middleinitial varchar(10),
Lastname varchar(50),
Gender varchar(10),
Housenumber varchar(20),
Streetname varchar(38),
Streetpostdirectional varchar(21),
Streetpredirectional varchar(21),
Streetsuffix varchar(41),
Unitnumber varchar(81),
Unittype varchar(41),
City varchar(28),
State varchar(2),
Zipcode varchar(5),
Zipfour varchar(4),
Zipfourtype varchar(11),
CBSACode varchar(41),
Latitude varchar(11),
Longitude varchar(11),
Fipscountycode varchar(3),
Fipsstatecode varchar(2),
Cordcutter varchar(1),
Prepaid_Indicator varchar(1),
Verified_Code varchar(1),
Activitystatus varchar(2),
Filterflag varchar(1),
Filterreason varchar(2),
Isdqi varchar(1),
Matchlevel varchar(7),
IndividualMatch varchar(1),
Matchscore varchar(3),
Modifiedscore varchar(3),
Lemsmatchcode varchar(18),
Company_ID bigint,
Individual_ID bigint,
Donotcallflag varchar(1),
ID int IDENTITY
) DISTSTYLE ALL
;

copy {wireless-tablename1}
from 's3://{s3-axle-gold}/wireless/{wireless-input-tablename}'
iam_role '{iam}'
delimiter '|' ;



