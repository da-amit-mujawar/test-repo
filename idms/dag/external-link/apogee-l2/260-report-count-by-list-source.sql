unload ('select o.* from (select L2_ListID, L2_PUBCODE, L2_PubName, 
		count(*) as Count, 
		sum(case when individual_id =0 then 0 else 1 end) as GrossDQIMatches
from {tablename1} 
group by L2_ListID, L2_PUBCODE, L2_PubName
order by L2_PUBCODE, L2_PubName ) as o')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;