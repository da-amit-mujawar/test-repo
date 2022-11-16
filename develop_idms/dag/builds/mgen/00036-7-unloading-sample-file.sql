unload('
select
rtrim(tblchild.cid),
rtrim(trs_supercategory),
rtrim(trs_recency),
rtrim(trs_categorydatecode),
rtrim(trs_totaldollar12month),
rtrim(trs_totaldollar12monthcode),
rtrim(trs_averagedollar12month),
rtrim(trs_totaltransaction12month),
rtrim(trs_totaltransaction12monthcode),
rtrim(trs_totaldollar13plus),
rtrim(trs_totaltransaction13plus)
from onepercent_modelsample_{build_id}
inner join tblchild4_{build_id}_{build} tblchild
on onepercent_modelsample_{build_id}.cid = tblchild.cid
')
to 's3://{s3-internal}/neptune/mGen/ModelSample/build_{build}/mgen-1p/supercat_trans'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
parallel off
CSV DELIMITER AS '|'
allowoverwrite
gzip;