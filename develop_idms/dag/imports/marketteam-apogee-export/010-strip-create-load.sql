--create stat table
DROP TABLE IF EXISTS {table_job_stats};

CREATE TABLE {table_job_stats}
(task varchar(150),quantity bigint, run_date timestamp sortkey);


DROP TABLE IF EXISTS {tablename1};

CREATE TABLE {tablename1}
    (individualid1 varchar(12) not null default '',
    familyid varchar(12) not null default '',
    individualid2 varchar(12) not null default '',
    individualid3 varchar(12) not null default '',
    individualid4 varchar(12) not null default '');

--load fixed-width file no header
COPY {tablename1}
from 's3://{s3-internal}/neptune/mtm_strip/apo_hhid.txt.gz'
--FROM 's3://idms-2722-playground/elina/apo_hhid.txt.gz'
IAM_ROLE '{iam}'
GZIP
ACCEPTINVCHARS
FIXEDWIDTH
'individualid1:12,
familyid:12,
individualid2:12,
individualid3:12,
individualid4:12';

INSERT INTO {table_job_stats}
SELECT 'MTM Consumer Strip file loaded',count(*),getdate() FROM {tablename1};

--restructure mtm consumer strip file
DROP TABLE IF EXISTS {tablename2};

CREATE TABLE {tablename2}
    (individualid varchar(12) not null default '',
    familyid varchar(12) not null default '');

--MEM 1
INSERT INTO {tablename2}
    (
	individualid,
	familyid
    )
SELECT
individualid1, familyid
FROM {tablename1}
WHERE individualid1<>' '
GROUP BY individualid1, familyid;

--MEM 2
INSERT INTO {tablename2}
    (
	individualid,
	familyid
    )
SELECT
individualid2, familyid
FROM {tablename1}
WHERE individualid2<>' '
GROUP BY individualid2, familyid;

--MEM 3
INSERT INTO {tablename2}
    (
	individualid,
	familyid
    )
SELECT
individualid3, familyid
FROM {tablename1}
WHERE individualid3<>' '
GROUP BY individualid3, familyid;

--MEM 4

INSERT INTO {tablename2}
    (
	individualid,
	familyid
    )
SELECT
individualid4, familyid
FROM {tablename1}
WHERE individualid4<>' '
GROUP BY individualid4, familyid;

--count reformat table
INSERT INTO {table_job_stats}
SELECT 'MTM Consumer Reformatted file loaded',count(*),getdate() FROM {tablename2};
