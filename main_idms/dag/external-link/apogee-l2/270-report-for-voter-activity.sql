unload ('select o.* from (select L2_Voters_Active, L2_PRI_BLT_2016, count(*) as TotalRecords
from  {tablename1}
group by L2_Voters_Active, L2_PRI_BLT_2016
order by L2_Voters_Active, L2_PRI_BLT_2016 ) as o')
to 's3://{s3-internal}{reportname3}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;