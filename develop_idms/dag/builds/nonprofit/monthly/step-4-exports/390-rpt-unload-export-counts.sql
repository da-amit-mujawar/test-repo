unload ('select * from apogee_export_count')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off;
/*
drop table if exists ctas_cons_unv_compid_tobedropped;
drop table if exists ctas_cons_unv_individ_tobedropped;

drop table if exists ctas_tbldonorflag_tobedropped;
drop table if exists apogeepoliticalpivot_tobedropped;
drop table if exists ctas_political_donor_tobedropped;

drop table if exists nonprofit_summary_tobedropped;
drop table if exists fec_donor_hh_summary_tobedropped;
drop table if exists comb_nonprofit_tobedropped;
drop table if exists nonprofit_mgen_tobedropped;
drop table if exists l2_voter_apg_exp_tobedropped;
drop table if exists ctas_ext39_mmdb_tobedropped;
*/