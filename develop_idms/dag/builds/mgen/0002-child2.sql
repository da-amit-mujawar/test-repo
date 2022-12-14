drop table if exists tblchild2_{build_id}_{build} CASCADE;

create table tblchild2_{build_id}_{build}
(
lemsmatchcode VARCHAR(18) ENCODE ZSTD,
cid VARCHAR(18) ENCODE RAW,
dqi_id VARCHAR(12) ENCODE ZSTD,
child1_dob VARCHAR(6) ENCODE ZSTD,
child1_gender VARCHAR(1) ENCODE ZSTD,
child2_dob VARCHAR(6) ENCODE ZSTD,
child2_gender VARCHAR(1) ENCODE ZSTD,
child3_dob VARCHAR(6) ENCODE ZSTD,
child3_gender VARCHAR(1) ENCODE ZSTD,
child4_dob VARCHAR(6) ENCODE ZSTD,
child4_gender VARCHAR(1) ENCODE ZSTD,
child5_dob VARCHAR(6) ENCODE ZSTD,
child5_gender VARCHAR(1) ENCODE ZSTD,
congressionaldistrict VARCHAR(2) ENCODE ZSTD,
buyer_behavior_cluster_2004 VARCHAR(10) ENCODE ZSTD,
locationtype VARCHAR(1) ENCODE ZSTD,
emailpresenceflag VARCHAR(1) ENCODE ZSTD,
grandparentinhouse VARCHAR(1) ENCODE ZSTD,
homeequityestimate VARCHAR(1) ENCODE ZSTD,
income_code VARCHAR(1) ENCODE ZSTD,
homevalue VARCHAR(1) ENCODE ZSTD,
findincome_old VARCHAR(6) ENCODE BYTEDICT,
lengthofresidence_old VARCHAR(2) ENCODE BYTEDICT,
length_of_residence_code VARCHAR(1) ENCODE ZSTD,
lines_of_credit_code VARCHAR(1) ENCODE ZSTD,
mortgageamount VARCHAR(1) ENCODE ZSTD,
wealthfinder VARCHAR(1) ENCODE ZSTD,
nielsencountyrank VARCHAR(1) ENCODE ZSTD,
numberofadults VARCHAR(1) ENCODE ZSTD,
numberofchildren VARCHAR(1) ENCODE ZSTD,
femaleoccupation VARCHAR(2) ENCODE ZSTD,
maleoccupation VARCHAR(2) ENCODE ZSTD,
ownrent VARCHAR(1) ENCODE ZSTD,
potentialinvestorconsumer VARCHAR(2) ENCODE ZSTD,
presenceofchildren VARCHAR(1) ENCODE ZSTD,
revolverminimumpaymentmodel VARCHAR(2)  ENCODE ZSTD,
phone1 VARCHAR(10) ENCODE ZSTD,
phonenumbertype VARCHAR(1) ENCODE ZSTD,
workathomeflag VARCHAR(1) ENCODE ZSTD,
internetusage VARCHAR(2) ENCODE ZSTD,
heavy_internet_user VARCHAR(2) ENCODE ZSTD,
dmhightechhousehold VARCHAR(1) ENCODE ZSTD,
hohagecode VARCHAR(1) ENCODE ZSTD,
ppi_old VARCHAR(6) ENCODE BYTEDICT,
ppi_code VARCHAR(1) ENCODE ZSTD,
prizm_ne_cluster_code VARCHAR(2) ENCODE ZSTD,
bbc_2004 VARCHAR(2) ENCODE ZSTD,
age0_3 VARCHAR(1) ENCODE ZSTD,
wirelessflag VARCHAR(1) ENCODE ZSTD,
nielsencountyregion VARCHAR(1) ENCODE ZSTD,
veteraninhhld VARCHAR(1) ENCODE ZSTD,
address_type VARCHAR(1) ENCODE ZSTD,
household_head_age_code_source VARCHAR(1) ENCODE ZSTD,
consumer_stability_index_raw_score VARCHAR(3) ENCODE BYTEDICT,
corrective_lenses_present_flag VARCHAR(1) ENCODE ZSTD,
individual_hoh_head_of_household_flag VARCHAR(1) ENCODE ZSTD,
expendable_income_rank_code VARCHAR(1) ENCODE ZSTD,
home_owner_flag VARCHAR(1) ENCODE ZSTD,
family_income_detector_code VARCHAR(1) ENCODE ZSTD,
mail_order_software_purchase_flag VARCHAR(1) ENCODE ZSTD,
mail_order_buyer_current_flag VARCHAR(1) ENCODE ZSTD,
mail_order_buyer_ever_flag VARCHAR(1) ENCODE ZSTD,
household_head_married_score VARCHAR(1) ENCODE ZSTD,
surnames_in_household VARCHAR(1) ENCODE ZSTD,
occupancy_count VARCHAR(2) ENCODE ZSTD,
als_state_code_2010 VARCHAR(2) ENCODE ZSTD,
als_county_code_2010 VARCHAR(3) ENCODE ZSTD,
als_census_tract_2010 VARCHAR(6) ENCODE ZSTD,
als_census_bg_2010 VARCHAR(1) ENCODE ZSTD,
alsmedianfind_2010 VARCHAR(3) ENCODE BYTEDICT,
alsmedianlor_2010 VARCHAR(2) ENCODE ZSTD,
alsmedianwealth_2010 VARCHAR(1) ENCODE ZSTD,
alsmedianageheadhh_2010 VARCHAR(2) ENCODE ZSTD,
alssesicode_2010 VARCHAR(2) ENCODE ZSTD,
alsearlyinternet_2010 VARCHAR(4) ENCODE ZSTD,
alsheavyinternet_2010 VARCHAR(4) ENCODE ZSTD,
delivery_unit_size VARCHAR(2) ENCODE ZSTD,
family_income_detector_ranges VARCHAR(1) ENCODE ZSTD,
home_age VARCHAR(3) ENCODE BYTEDICT,
home_sale_date VARCHAR(8) ENCODE ZSTD,
revolver_to_transactor_model VARCHAR(2) ENCODE ZSTD,
lengthofresidence INT ENCODE LZO,
ppi INT ENCODE LZO,
findincome INT ENCODE LZO
)
DISTSTYLE KEY
DISTKEY(cid)
SORTKEY(cid);

copy tblchild2_{build_id}_{build}
(
	lemsmatchcode,
	cid,
	dqi_id,
	child1_dob,
	child1_gender,
	child2_dob,
	child2_gender,
	child3_dob,
	child3_gender,
	child4_dob,
	child4_gender,
	child5_dob,
	child5_gender,
	congressionaldistrict,
	buyer_behavior_cluster_2004,
	locationtype,
	emailpresenceflag,
	grandparentinhouse,
	homeequityestimate,
	income_code,
	homevalue,
	findincome_old,
	lengthofresidence_old,
	length_of_residence_code,
	lines_of_credit_code,
	mortgageamount,
	wealthfinder,
	nielsencountyrank,
	numberofadults,
	numberofchildren,
	femaleoccupation,
	maleoccupation,
	ownrent,
	potentialinvestorconsumer,
	presenceofchildren,
	revolverminimumpaymentmodel,
	phone1,
	phonenumbertype,
	workathomeflag,
	internetusage,
	heavy_internet_user,
	dmhightechhousehold,
	hohagecode,
	ppi_old,
	ppi_code,
	prizm_ne_cluster_code,
	bbc_2004,
	age0_3,
	wirelessflag,
	nielsencountyregion,
	veteraninhhld,
	address_type,
	household_head_age_code_source,
	consumer_stability_index_raw_score,
	corrective_lenses_present_flag,
	individual_hoh_head_of_household_flag,
	expendable_income_rank_code,
	home_owner_flag,
	family_income_detector_code,
	mail_order_software_purchase_flag,
	mail_order_buyer_current_flag,
	mail_order_buyer_ever_flag,
	household_head_married_score,
	surnames_in_household,
	occupancy_count,
	als_state_code_2010,
	als_county_code_2010,
	als_census_tract_2010,
	als_census_bg_2010,
	alsmedianfind_2010,
	alsmedianlor_2010,
	alsmedianwealth_2010,
	alsmedianageheadhh_2010,
	alssesicode_2010,
	alsearlyinternet_2010,
	alsheavyinternet_2010,
	delivery_unit_size,
	family_income_detector_ranges,
	home_age,
	home_sale_date,
	revolver_to_transactor_model
)
from 's3://{s3-internal}/neptune/mGen/tblChild2_1196_201301.txt'
iam_role '{iam}'
fixedwidth 
'lemsmatchcode:18,
cid:18,
dqi_id:12,
child1_dob:6,
child1_gender:1,
child2_dob:6,
child2_gender:1,
child3_dob:6,
child3_gender:1,
child4_dob:6,
child4_gender:1,
child5_dob:6,
child5_gender:1,
congressionaldistrict:2,
buyer_behavior_cluster_2004:2,
locationtype:1,
emailpresenceflag:1,
grandparentinhouse:1,
homeequityestimate:1,
income_code:1,
homevalue:1,
findincome:6,
lengthofresidence:2,
length_of_residence_code:1,
lines_of_credit_code:1,
mortgageamount:1,
wealthfinder:1,
nielsencountyrank:1,
numberofadults:1,
numberofchildren:1,
femaleoccupation:2,
maleoccupation:2,
ownrent:1,
potentialinvestorconsumer:2,
presenceofchildren:1,
revolverminimumpaymentmodel:2,
phone1:10,
phonenumbertype:1,
workathomeflag:1,
internetusage:2,
heavy_internet_user:2,
dmhightechhousehold:1,
hohagecode:1,
ppi:6,
ppi_code:1,
prizm_ne_cluster_code:2,
bbc_2004:2,
age0_3:1,
wirelessflag:1,
nielsencountyregion:1,
veteraninhhld:1,
address_type:1,
household_head_age_code_source:1,
consumer_stability_index_raw_score:3,
corrective_lenses_present_flag:1,
individual_hoh_head_of_household_flag:1,
expendable_income_rank_code:1,
home_owner_flag:1,
family_income_detector_code:1,
mail_order_software_purchase_flag:1,
mail_order_buyer_current_flag:1,
mail_order_buyer_ever_flag:1,
household_head_married_score:1,
surnames_in_household:1,
occupancy_count:2,
als_state_code_2010:2,
als_county_code_2010:3,
als_census_tract_2010:6,
als_census_bg_2010:1,
alsmedianfind_2010:3,
alsmedianlor_2010:2,
alsmedianwealth_2010:1,
alsmedianageheadhh_2010:2,
alssesicode_2010:2,
alsearlyinternet_2010:4,
alsheavyinternet_2010:4,
delivery_unit_size:2,
family_income_detector_ranges:1,
home_age :3,
home_sale_date :8,
revolver_to_transactor_model :2'
;

update tblchild2_{build_id}_{build} 
set findincome = case when findincome_old is not null and findincome_old <> '' then cast(findincome_old as int) else 0 end,
    ppi = case when ppi_old is null or ppi_old = '' then 0 else cast(ppi_old as int) end,
    lengthofresidence = case when lengthofresidence_old is null or lengthofresidence_old = '' then 0 else cast(lengthofresidence_old as int) end,
    wirelessflag = case when wirelessflag is null or wirelessflag = '' then 'N' ELSE wirelessflag end; 

UNLOAD ('select * from tblchild2_{build_id}_{build}')
TO 's3://{s3-axle-gold}/mgen/{s3_folder_today}/mgen-dqi/'
iam_role '{iam}'
manifest verbose
FORMAT PARQUET;
