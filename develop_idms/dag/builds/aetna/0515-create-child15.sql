DROP VIEW IF EXISTS tblChild15_{build_id}_{build};
CREATE VIEW tblChild15_{build_id}_{build}
AS
SELECT 
IA.MZB_INDIV_ID FROM public.TBMZB_INDIV_ACA_LIST I left outer join public.tbmzb_individual_merge_fix mrgd  
on i.mzb_indiv_id=mrgd.MRGD_MZB_INDIV_ID join public.TBMZB_INDIVIDUAL_A IA on nvl(mrgd.SURV_MZB_INDIV_ID,I.mzb_indiv_id)=IA.mzb_indiv_id
with no schema binding;