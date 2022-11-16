DROP VIEW IF EXISTS tblChild14_{build_id}_{build};
CREATE VIEW tblChild14_{build_id}_{build}
AS
SELECT
I.PS_UNIQUE_ID,
I.ULT_DUNS_NBR,
I.BUS_IGID,
I.INDIVIDUALID,
IA.MZB_INDIV_ID,
I.INDIV_TYPE_IND,
I.HH_CNT FROM public.TBMZB_B2C_DA_CONSUMERS I left outer join public.tbmzb_individual_merge_fix mrgd  
on i.mzb_indiv_id=mrgd.MRGD_MZB_INDIV_ID join public.TBMZB_INDIVIDUAL_A IA on nvl(mrgd.SURV_MZB_INDIV_ID,I.mzb_indiv_id)=IA.mzb_indiv_id
with no schema binding;