
DROP TABLE IF EXISTS tblExternal16_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal17_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal18_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal19_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal20_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal21_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal22_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal27_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal28_191_201206_Rollup;
DROP TABLE IF EXISTS tblExternal3_191_201206_NEW;


CREATE TABLE tblExternal16_191_201206_Rollup (lems varchar(18) PRIMARY KEY encode raw ,	children_age_presence_flag	Varchar(50) encode zstd ) DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS);
copy tblExternal16_191_201206_Rollup (lems , children_age_presence_flag)
from 's3://{s3-internal}{s3-key2}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal17_191_201206_Rollup (lems varchar(18) PRIMARY KEY encode raw,	interest_flag	Varchar(150)encode zstd ) DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS);
copy tblExternal17_191_201206_Rollup (lems , interest_flag)
from 's3://{s3-internal}{s3-key3}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal18_191_201206_Rollup (lems  varchar(18) PRIMARY KEY encode raw,	lifestyle_hobby_interest_flags	Varchar(360)encode zstd ) DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS);
copy tblExternal18_191_201206_Rollup (lems , lifestyle_hobby_interest_flags)
from 's3://{s3-internal}{s3-key4}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal19_191_201206_Rollup (lems  varchar(18) PRIMARY KEY encode raw,	ailments_flags	Varchar(100)encode zstd ) DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS)  ; 
copy tblExternal19_191_201206_Rollup (lems , ailments_flags)
from 's3://{s3-internal}{s3-key5}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal20_191_201206_Rollup (lems  varchar(18) PRIMARY KEY encode raw,	credit_card_flag	Varchar(50)encode zstd )  DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS);
copy tblExternal20_191_201206_Rollup (lems , credit_card_flag)
from 's3://{s3-internal}{s3-key6}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal21_191_201206_Rollup (lems  varchar(18) PRIMARY KEY encode raw,	donor_flags	Varchar(50)encode zstd ) DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS) ;
copy tblExternal21_191_201206_Rollup (lems , donor_flags)
from 's3://{s3-internal}{s3-key7}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal22_191_201206_Rollup (lems  varchar(18) PRIMARY KEY encode raw,	market_target_age_flags	Varchar(150) encode zstd) DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS) ;
copy tblExternal22_191_201206_Rollup (lems , market_target_age_flags)
from 's3://{s3-internal}{s3-key8}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal27_191_201206_Rollup (lems varchar(18) PRIMARY KEY encode raw,	children_month_of_birth	Varchar(500) encode zstd) DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS) ;
copy tblExternal27_191_201206_Rollup (lems , children_month_of_birth)
from 's3://{s3-internal}{s3-key9}' 
iam_role '{iam}'
delimiter '|';


CREATE TABLE tblExternal28_191_201206_Rollup (lems varchar(18) PRIMARY KEY encode raw,	children_Age_by_gender	Varchar(250) encode zstd)  DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS);
copy tblExternal28_191_201206_Rollup (lems , children_Age_by_gender)
from 's3://{s3-internal}{s3-key10}' 
iam_role '{iam}'
delimiter '|';


/*Target Ready  Link file.  We will be adding this to DQI Link.  Reju /Jayesh  2014.06.19*/

CREATE TABLE tblExternal3_191_201206_NEW (lems varchar(18) PRIMARY KEY encode raw,	CE_Marketing_Target_Models	Varchar(800) encode zstd  )  DISTSTYLE KEY DISTKEY(LEMS) SORTKEY(LEMS);
copy tblExternal3_191_201206_NEW (lems , CE_Marketing_Target_Models)
from 's3://{s3-internal}{s3-key11}' 
iam_role '{iam}'
delimiter '|';

--TRM_link.txt
--\\stcsanisln01-idms\idms\neptune\IDMSFILES\


DROP TABLE IF EXISTS {tablename2};

CREATE TABLE {tablename2}  
(
lems varchar(18) PRIMARY KEY encode raw,
children_age_presence_flag  Varchar(50) encode zstd,
interest_flag Varchar(150) encode zstd,
lifestyle_hobby_interest_flags Varchar(360) encode zstd ,
ailments_flags Varchar(100) encode zstd,
credit_card_flag Varchar(50) encode zstd,
donor_flags Varchar(50) encode zstd,
market_target_age_flags Varchar(150) encode zstd,
children_month_of_birth Varchar(500) encode zstd,
children_Age_by_gender Varchar(250) encode zstd,
CE_Marketing_Target_Models varchar(800) encode zstd
)
DISTSTYLE KEY
DISTKEY(LEMS)
SORTKEY(LEMS)  ;


/* Delete Blank Lems. Incident# 717686*/

DELETE FROM {tablename2} WHERE LTRIM(RTRIM(lems)) = '' OR lems IS NULL; 


INSERT INTO {tablename2} 
    (LEMS, 
     children_age_presence_flag, 
    interest_flag,
    lifestyle_hobby_interest_flags,
    ailments_flags,
    credit_card_flag,
    donor_flags,
    market_target_age_flags,
    children_month_of_birth,
    children_Age_by_gender ,
  CE_Marketing_Target_Models
  )
SELECT 
	tbl15.LEMS, 
	NVL(tbl16.children_age_presence_flag, ''),
	NVL(tbl17.interest_flag, ''),
	NVL(tbl18.lifestyle_hobby_interest_flags, ''),
	NVL(tbl19.ailments_flags, ''),
	NVL(tbl20.credit_card_flag, ''),
	NVL(tbl21.donor_flags, ''),
	NVL(tbl22.market_target_age_flags, ''),
	NVL(tbl27.children_month_of_birth, ''),
	NVL(tbl28.children_Age_by_gender, ''),
	NVL(TargetReadyLink.CE_Marketing_Target_Models, '')
FROM tblExternal15_191_201206_NEW  tbl15
LEFT JOIN tblExternal16_191_201206_Rollup  tbl16   ON tbl15.LEMS = tbl16.LEMS
LEFT JOIN tblExternal17_191_201206_Rollup  tbl17   ON tbl15.LEMS = tbl17.LEMS
LEFT JOIN tblExternal18_191_201206_Rollup  tbl18   ON tbl15.LEMS = tbl18.LEMS
LEFT JOIN tblExternal19_191_201206_Rollup  tbl19   ON tbl15.LEMS = tbl19.LEMS
LEFT JOIN tblExternal20_191_201206_Rollup  tbl20   ON tbl15.LEMS = tbl20.LEMS
LEFT JOIN tblExternal21_191_201206_Rollup  tbl21   ON tbl15.LEMS = tbl21.LEMS
LEFT JOIN tblExternal22_191_201206_Rollup  tbl22   ON tbl15.LEMS = tbl22.LEMS
LEFT JOIN tblExternal27_191_201206_Rollup  tbl27   ON tbl15.LEMS = tbl27.LEMS
LEFT JOIN tblExternal28_191_201206_Rollup  tbl28   ON tbl15.LEMS = tbl28.LEMS
LEFT JOIN tblExternal3_191_201206_NEW	   TargetReadyLink    ON tbl15.LEMS = TargetReadyLink.LEMS;


/*--finally, Drop Rollup tables*/

DROP TABLE tblExternal16_191_201206_Rollup ; 
DROP TABLE  tblExternal17_191_201206_Rollup ;
DROP TABLE  tblExternal18_191_201206_Rollup ;
DROP TABLE  tblExternal19_191_201206_Rollup ;
DROP TABLE  tblExternal20_191_201206_Rollup ;
DROP TABLE  tblExternal21_191_201206_Rollup ;
DROP TABLE  tblExternal22_191_201206_Rollup ;
DROP TABLE  tblExternal27_191_201206_Rollup ;
DROP TABLE  tblExternal28_191_201206_Rollup ;
DROP TABLE  tblExternal3_191_201206_NEW;

