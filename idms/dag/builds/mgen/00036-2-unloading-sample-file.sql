unload('
select
rtrim(tblchild.cid),
rtrim(trn_category),
rtrim(trn_supercategory),
rtrim(trn_recency),
rtrim(trn_categorydatecode),
rtrim(trn_totaldollar12month),
rtrim(trn_totaldollar12monthcode),
rtrim(trn_averagedollar12month),
rtrim(trn_totaltransaction12month),
rtrim(trn_totaltransaction12monthcode),
rtrim(trn_totaldollar13plus),
rtrim(trn_totaltransaction13plus),
rtrim(trn_product)
from modelsample_{build_id}
inner join tblchild5_{build_id}_{build} tblchild
on modelsample_{build_id}.cid = tblchild.cid
')
to 's3://{s3-internal}/neptune/mGen/ModelSample/build_{build}/mgen-50k/product_trans'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
parallel off
CSV DELIMITER AS '|'
allowoverwrite
gzip;
