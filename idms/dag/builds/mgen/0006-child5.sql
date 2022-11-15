DROP TABLE IF EXISTS tblchild5_{build_id}_{build} CASCADE;
CREATE TABLE tblchild5_{build_id}_{build} 
(
  cid VARCHAR(18) ENCODE ZSTD,
  trn_category VARCHAR(4)  ENCODE BYTEDICT,
  trn_supercategory VARCHAR(4) ENCODE ZSTD,
  trn_recency INT ENCODE AZ64,
  trn_categorydatecode VARCHAR(1) ENCODE ZSTD,
  trn_totaldollar12month INT ENCODE ZSTD,
  trn_totaldollar12monthcode VARCHAR(1) ENCODE ZSTD,
  trn_averagedollar12month INT ENCODE ZSTD,
  trn_totaltransaction12month INT ENCODE ZSTD,
  trn_totaltransaction12monthcode VARCHAR(1) ENCODE ZSTD,
  trn_totaldollar13plus INT ENCODE ZSTD,
  trn_totaltransaction13plus INT ENCODE ZSTD,
  trn_product VARCHAR(4) ENCODE ZSTD
)
DISTSTYLE KEY
DISTKEY(cid)
SORTKEY(cid);


COPY tblchild5_{build_id}_{build}
FROM 's3://{s3-internal}/neptune/mGen/TRN1.txt'
iam_role '{iam}'
fixedwidth 
'cid:18, 
trn_category:4,
trn_supercategory:4,
trn_recency:5, 
trn_categorydatecode:1,
trn_totaldollar12month:5,
trn_totaldollar12monthcode:1,
trn_averagedollar12month:5,
trn_totaltransaction12month:3,
trn_totaltransaction12monthcode:1,
trn_totaldollar13plus:5,
trn_totaltransaction13plus:3,
trn_product:4';

UNLOAD ('select * from tblchild5_{build_id}_{build}')
TO 's3://{s3-axle-gold}/mgen/{today}/mgen-transactions/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;

drop table if exists tblchild4_{build_id}_{build} CASCADE;

CREATE TABLE tblchild4_{build_id}_{build}
(
cid VARCHAR(18) ENCODE RAW,
trs_supercategory VARCHAR(4) ENCODE BYTEDICT,
trs_recency INT ENCODE AZ64,
trs_categorydatecode VARCHAR(1)  ENCODE ZSTD,
trs_totaldollar12month INT ENCODE ZSTD,
trs_totaldollar12monthcode VARCHAR(1) ENCODE ZSTD,
trs_averagedollar12month INT ENCODE LZO,
trs_totaltransaction12month INT ENCODE ZSTD,
trs_totaltransaction12monthcode VARCHAR(1) ENCODE ZSTD,
trs_totaldollar13plus INT ENCODE ZSTD,
trs_totaltransaction13plus INT ENCODE AZ64,
PRIMARY KEY(cid) 
)
DISTSTYLE KEY
DISTKEY(cid)
SORTKEY(cid);

INSERT INTO tblchild4_{build_id}_{build} 
(
    cid, 
    trs_supercategory, 
    trs_recency, 
    trs_categorydatecode, 
    trs_totaldollar12month, 
    trs_totaldollar12monthcode,  
    trs_totaltransaction12month, 
    trs_totaltransaction12monthcode, 
    trs_totaldollar13plus, 
    trs_totaltransaction13plus
)
SELECT 
cid,
trn_supercategory,
min(trn_recency),
min(trn_categorydatecode),
SUM(trn_totaldollar12month),
CASE WHEN SUM(trn_totaldollar12month) = 0 THEN '0'
     WHEN SUM(trn_totaldollar12month) > 0 AND SUM(trn_totaldollar12month) <= 24 THEN 'A'
     WHEN SUM(trn_totaldollar12month) >= 25 AND SUM(trn_totaldollar12month) <= 49 THEN 'B'
     WHEN SUM(trn_totaldollar12month) >= 50 AND SUM(trn_totaldollar12month) <= 74 THEN 'C'
     WHEN SUM(trn_totaldollar12month) >= 75 AND SUM(trn_totaldollar12month) <= 99 THEN 'D'
     WHEN SUM(trn_totaldollar12month) >= 100 AND SUM(trn_totaldollar12month) <= 149 THEN 'E'
     WHEN SUM(trn_totaldollar12month) >= 150 AND SUM(trn_totaldollar12month) <= 199 THEN 'F'
     WHEN SUM(trn_totaldollar12month) >= 200 AND SUM(trn_totaldollar12month) <= 299 THEN 'G'
     WHEN SUM(trn_totaldollar12month) >= 300 AND SUM(trn_totaldollar12month) <= 499 THEN 'H'
     WHEN SUM(trn_totaldollar12month) >= 500 AND SUM(trn_totaldollar12month) <= 749 THEN 'I'
     WHEN SUM(trn_totaldollar12month) >= 750 AND SUM(trn_totaldollar12month) <= 999 THEN 'J'
     WHEN SUM(trn_totaldollar12month) >= 1000 AND SUM(trn_totaldollar12month) <= 1499 THEN 'K'
     WHEN SUM(trn_totaldollar12month) >= 1500 AND SUM(trn_totaldollar12month) <= 1999 THEN 'L'
ELSE 'M' END,
SUM(trn_totaltransaction12month),        
CASE WHEN SUM(trn_totaltransaction12month) IS NULL THEN '0'
     WHEN SUM(trn_totaltransaction12month) = 0 THEN '0'
     WHEN SUM(trn_totaltransaction12month) = 1 THEN 'A'
     WHEN SUM(trn_totaltransaction12month) = 2 THEN 'B'
     WHEN SUM(trn_totaltransaction12month) = 3 THEN 'C'
     WHEN SUM(trn_totaltransaction12month) IN (4,5) THEN 'D'
     WHEN SUM(trn_totaltransaction12month) IN (6,7,8,9) THEN 'E'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 10 AND 14 THEN 'F'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 15 AND 19 THEN 'G'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 20 AND 29 THEN 'H'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 30 AND 39 THEN 'I'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 40 AND 49 THEN 'J'
ELSE 'K' END, 
SUM(trn_totaldollar13plus),
SUM(trn_totaltransaction13plus)
FROM tblchild5_{build_id}_{build} 
WHERE trn_supercategory IS NOT NULL AND trn_supercategory <> ''
GROUP BY cid,trn_supercategory; 

UPDATE tblchild4_{build_id}_{build} SET trs_averagedollar12month = CASE WHEN (trs_totaltransaction12month) > 1 
THEN floor((trs_totaldollar12month)/(trs_totaltransaction12month)) ELSE (trs_totaldollar12month) END;

UNLOAD ('select * from tblchild4_{build_id}_{build}')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-transactions-sc/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;  

DROP TABLE IF exists tblchild7_{build_id}_{build} CASCADE;
CREATE TABLE tblchild7_{build_id}_{build}
(
cid VARCHAR(18) ENCODE ZSTD,
trc_category VARCHAR(4)  ENCODE BYTEDICT,
trc_recency INT   ENCODE AZ64,
trc_categorydatecode VARCHAR(1)  ENCODE ZSTD,
trc_totaldollar12month INT  ENCODE ZSTD,
trc_totaldollar12monthcode VARCHAR(1) ENCODE ZSTD,
trc_averagedollar12month INT  ENCODE LZO,
trc_totaltransaction12month INT  ENCODE ZSTD,
trc_totaltransaction12monthcode VARCHAR(1)  ENCODE ZSTD,
trc_totaldollar13plus INT  ENCODE ZSTD,
trc_totaltransaction13plus INT ENCODE ZSTD,
PRIMARY KEY(cid)
);

INSERT INTO tblChild7_{build_id}_{build}
(
cid, 
trc_category, 
trc_recency, 
trc_categorydatecode, 
trc_totaldollar12month, 
trc_totaldollar12monthcode,
trc_totaltransaction12month, 
trc_totaltransaction12monthcode, 
trc_totaldollar13plus, 
trc_totaltransaction13plus
)
SELECT 
cid,
trn_category,
MIN(trn_recency),
MIN(trn_categorydatecode),
SUM(trn_totaldollar12month),
CASE WHEN SUM(trn_totaldollar12month) = 0 THEN '0'
     WHEN SUM(trn_totaldollar12month) > 0 and SUM(trn_totaldollar12month) <= 24 THEN 'A'
     WHEN SUM(trn_totaldollar12month) >= 25 and SUM(trn_totaldollar12month) <= 49 THEN 'B'
     WHEN SUM(trn_totaldollar12month) >= 50 and SUM(trn_totaldollar12month) <= 74 THEN 'C'
     WHEN SUM(trn_totaldollar12month) >= 75 and SUM(trn_totaldollar12month) <= 99 THEN 'D'
     WHEN SUM(trn_totaldollar12month) >= 100 and SUM(trn_totaldollar12month) <= 149 THEN 'E'
     WHEN SUM(trn_totaldollar12month) >= 150 and SUM(trn_totaldollar12month) <= 199 THEN 'F'
     WHEN SUM(trn_totaldollar12month) >= 200 and SUM(trn_totaldollar12month) <= 299 THEN 'G'
     WHEN SUM(trn_totaldollar12month) >= 300 and SUM(trn_totaldollar12month) <= 499 THEN 'H'
     WHEN SUM(trn_totaldollar12month) >= 500 and SUM(trn_totaldollar12month) <= 749 THEN 'I'
     WHEN SUM(trn_totaldollar12month) >= 750 and SUM(trn_totaldollar12month) <= 999 THEN 'J'
     WHEN SUM(trn_totaldollar12month) >= 1000 and SUM(trn_totaldollar12month) <= 1499 THEN 'K'
     WHEN SUM(trn_totaldollar12month) >= 1500 and SUM(trn_totaldollar12month) <= 1999 THEN 'L'
ELSE 'M' END,
SUM(trn_totaltransaction12month),
CASE WHEN SUM(trn_totaltransaction12month) IS null THEN '0'
     WHEN SUM(trn_totaltransaction12month) = 0 THEN '0'
     WHEN SUM(trn_totaltransaction12month) = 1 THEN 'A'
     WHEN SUM(trn_totaltransaction12month) = 2 THEN 'B'
     WHEN SUM(trn_totaltransaction12month) = 3 THEN 'C'
     WHEN SUM(trn_totaltransaction12month) IN (4,5) THEN 'D'
     WHEN SUM(trn_totaltransaction12month) IN (6,7,8,9) THEN 'E'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 10 AND 14 THEN 'F'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 15 AND 19 THEN 'G'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 20 AND 29 THEN 'H'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 30 AND 39 THEN 'I'
     WHEN SUM(trn_totaltransaction12month) BETWEEN 40 AND 49 THEN 'J'
ELSE 'K' END,
SUM(trn_totaldollar13plus),
SUM(trn_totaltransaction13plus)
FROM tblChild5_{build_id}_{build}
WHERE trn_category IS NOT NULL AND trn_category <> ''
GROUP BY cid,trn_category;


update tblChild7_{build_id}_{build}
  SET trc_averagedollar12month = CASE WHEN trc_totaltransaction12month > 1 THEN floor(trc_totaldollar12month/trc_totaltransaction12month) else trc_totaldollar12month end;


