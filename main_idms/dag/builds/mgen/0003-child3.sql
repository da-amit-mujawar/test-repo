drop table if exists tblchild3_{build_id}_{build} CASCADE;

create table tblchild3_{build_id}_{build}
(
expid VARCHAR(12) ENCODE ZSTD,
cid VARCHAR(18) ENCODE RAW,
experian_income VARCHAR(1) ENCODE ZSTD,
experian_enhanced_income VARCHAR(1) ENCODE ZSTD,
experian_person1_dob VARCHAR(6) ENCODE ZSTD,
experian_estimated_age_old VARCHAR(2) ENCODE ZSTD,
experian_exact_age_old VARCHAR(2) ENCODE BYTEDICT,
experian_presence_of_children_type VARCHAR(1) ENCODE ZSTD,
experian_presence_of_children_code VARCHAR(1) ENCODE ZSTD,
experian_presence_of_child_0_18 VARCHAR(2) ENCODE ZSTD,
experian_ethnic_ethnic VARCHAR(2) ENCODE ZSTD,
experian_ethnic_experian_group VARCHAR(2) ENCODE ZSTD,
experian_ethnic_religion VARCHAR(1) ENCODE ZSTD,
experian_ethnic_language_preference VARCHAR(2) ENCODE ZSTD,
experian_ethnic_group VARCHAR(1) ENCODE ZSTD,
experian_ethnic_country_of_origin VARCHAR(2)  ENCODE ZSTD,
experian_matchcode VARCHAR(25) ENCODE ZSTD, 
experian_append_flag VARCHAR(1) ENCODE ZSTD, 
experian_donor_any VARCHAR(1) ENCODE ZSTD,
experian_child_any VARCHAR(1) ENCODE ZSTD,
experian_estimated_age INT ENCODE LZO,
experian_exact_age INT ENCODE LZO
)
DISTSTYLE KEY
DISTKEY(cid)
SORTKEY(cid);

copy tblchild3_{build_id}_{build}
(
    expid,
	cid,
	experian_income,
	experian_enhanced_income,
	experian_person1_dob,
	experian_estimated_age_old,
	experian_exact_age_old,
	experian_presence_of_children_type,
	experian_presence_of_children_code,
	experian_presence_of_child_0_18,
	experian_ethnic_ethnic,
	experian_ethnic_experian_group,
	experian_ethnic_religion,
	experian_ethnic_language_preference,
	experian_ethnic_group,
	experian_ethnic_country_of_origin,
	experian_matchcode,
	experian_append_flag,
	experian_donor_any,
	experian_child_any
)
from 's3://{s3-internal}/neptune/mGen/tblChild3_1196_201301.txt'
iam_role '{iam}'
fixedwidth
'expid:12,
cid:18,
experian_income:1,
experian_enhanced_income:1,
experian_person1_dob:6,
experian_estimated_age:2,
experian_exact_age:2,
experian_presence_of_children_type:1,
experian_presence_of_children_code:1,
experian_presence_of_child_0_18:2,
experian_ethnic_ethnic:2,
experian_ethnic_experian_group:2,
experian_ethnic_religion:1,
experian_ethnic_language_preference:2,
experian_ethnic_group:1,
experian_ethnic_country_of_origin:2,
experian_matchcode:25,
experian_append_flag:1,
experian_donor_any:1,
experian_child_any:1'
;

update tblchild3_{build_id}_{build}
set experian_estimated_age = case when experian_estimated_age_old is null or experian_estimated_age_old = '' then 0 else cast(experian_estimated_age_old as int) end,
    experian_exact_age = case when experian_exact_age_old is null or experian_exact_age_old = '' then 0 else cast(experian_exact_age_old as int) end;


UNLOAD ('select * from tblchild3_{build_id}_{build}')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-experian/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
