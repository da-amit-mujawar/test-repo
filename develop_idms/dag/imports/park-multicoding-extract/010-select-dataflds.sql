drop table if exists count_{order_id}_MultiBuyerCount_ByEmail_park_tobedropped;
create table count_{order_id}_MultiBuyerCount_ByEmail_park_tobedropped  as SELECT DISTINCT
A.individual_id, B.MultiBuyerCount_ByEmail,B.EmailAddress
from count_{order_id} A
--inner join tblMain_23091_202203 B
inner join {maintable_name} B
ON a.Individual_ID = b.Individual_ID
where B.EmailAddress <> ''
ORDER BY 1,2;

--select top 1000 * from count_1335106_park_tobedropped order by MultiBuyerCount_ByEmail desc;

unload ('SELECT * from count_{order_id}_MultiBuyerCount_ByEmail_park_tobedropped')
to 's3://{s3-internal}/Reports/out/IDMSOrder_MultiBuyer_{order_id}.txt'
iam_role '{iam}'
kms_key_id '{kmskey}'
ALLOWOVERWRITE
encrypted
parallel off
csv delimiter as '|';
--fixedwidth as 'individual_id:21, MultiBuyerCount_ByEmail:7,EmailAddress:65';



--download txt file from s3://idms-7933-internalfiles/Reports/out/ bucket and delete column 29 in ultra edit


--\\\\stcsanisln01-idms\\idms\\IDMSFileShare\\Sapphire\\Park Export\\IDMSOrder_MultiBuyer_1335106.txt
--select max(length(individual_id)),max(length(MultiBuyerCount_ByEmail)),max(length(EmailAddress)) from count_1335106_park_tobedropped;
--17	2	63

--interest area
drop table if exists count_{order_id}_InterestArea_park_tobedropped;
create table count_{order_id}_InterestArea_park_tobedropped  as SELECT DISTINCT
A.individual_id, b.InterestArea
from count_{order_id} A
--inner join tblMain_23091_202203 B
inner join {maintable_name} B
ON a.Individual_ID = b.Individual_ID
where b.InterestArea is not null
ORDER BY 1,2;



unload ('SELECT * from count_{order_id}_InterestArea_park_tobedropped')
to 's3://{s3-internal}/Reports/out/IDMSOrder_InterestArea_{order_id}.txt'
iam_role '{iam}'
kms_key_id '{kmskey}'
ALLOWOVERWRITE
encrypted
parallel off
csv delimiter as '|';
--22484990
--ProductSpecified
drop table if exists count_{order_id}_ProductSpecified_park_tobedropped;
create table count_{order_id}_ProductSpecified_park_tobedropped  as SELECT DISTINCT
A.individual_id, b.ProductSpecified
from count_{order_id} A
--inner join tblMain_23091_202203 B
inner join {maintable_name} B
ON a.Individual_ID = b.Individual_ID
where b.ProductSpecified is not null
ORDER BY 1,2;



unload ('SELECT * from count_{order_id}_ProductSpecified_park_tobedropped')
to 's3://{s3-internal}/Reports/out/IDMSOrder_ProductSpecified_{order_id}.txt'
iam_role '{iam}'
kms_key_id '{kmskey}'
ALLOWOVERWRITE
encrypted
parallel off
csv delimiter as '|';

--17555280

--Specialty
drop table if exists count_{order_id}_Specialty_park_tobedropped;
create table count_{order_id}_Specialty_park_tobedropped  as SELECT DISTINCT
A.individual_id, b.Specialty
from count_{order_id} A
--inner join tblMain_23091_202203 B
inner join {maintable_name} B
ON a.Individual_ID = b.Individual_ID
where b.Specialty is not null
ORDER BY 1,2;



unload ('SELECT * from count_{order_id}_Specialty_park_tobedropped')
to 's3://{s3-internal}/Reports/out/IDMSOrder_Specialty_{order_id}.txt'
iam_role '{iam}'
kms_key_id '{kmskey}'
ALLOWOVERWRITE
encrypted
parallel off
csv delimiter as '|';

--15642379

--HardwareOnsite
drop table if exists count_{order_id}_HardwareOnsite_park_tobedropped;
create table count_{order_id}_HardwareOnsite_park_tobedropped  as SELECT DISTINCT
A.individual_id, b.HardwareOnsite
from count_{order_id} A
--inner join tblMain_23091_202203 B
inner join {maintable_name} B
ON a.Individual_ID = b.Individual_ID
where b.HardwareOnsite is not null
ORDER BY 1,2;



unload ('SELECT * from count_{order_id}_HardwareOnsite_park_tobedropped')
to 's3://{s3-internal}/Reports/out/IDMSOrder_HardwareOnsite_{order_id}.txt'
iam_role '{iam}'
kms_key_id '{kmskey}'
ALLOWOVERWRITE
encrypted
parallel off
csv delimiter as '|';

--119192656
--SoftwareOnsite
drop table if exists count_{order_id}_SoftwareOnsite_park_tobedropped;
create table count_{order_id}_SoftwareOnsite_park_tobedropped  as SELECT DISTINCT
A.individual_id, b.SoftwareOnsite
from count_{order_id} A
--inner join tblMain_23091_202203 B
inner join {maintable_name} B
ON a.Individual_ID = b.Individual_ID
where b.SoftwareOnsite is not null
ORDER BY 1,2;



unload ('SELECT * from count_{order_id}_SoftwareOnsite_park_tobedropped')
to 's3://{s3-internal}/Reports/out/IDMSOrder_SoftwareOnsite_{order_id}.txt'
iam_role '{iam}'
kms_key_id '{kmskey}'
ALLOWOVERWRITE
encrypted
parallel off
csv delimiter as '|';
--119192656

--jobfunction
drop table if exists count_{order_id}_jobfunction_park_tobedropped;
create table count_{order_id}_jobfunction_park_tobedropped  as SELECT DISTINCT
A.individual_id, b.jobfunction
from count_{order_id} A
--inner join tblMain_23091_202203 B
inner join {maintable_name} B
ON a.Individual_ID = b.Individual_ID
where b.jobfunction is not null
ORDER BY 1,2;



unload ('SELECT * from count_{order_id}_jobfunction_park_tobedropped')
to 's3://{s3-internal}/Reports/out/IDMSOrder_jobfunction_{order_id}.txt'
iam_role '{iam}'
kms_key_id '{kmskey}'
ALLOWOVERWRITE
encrypted
parallel off
csv delimiter as '|';