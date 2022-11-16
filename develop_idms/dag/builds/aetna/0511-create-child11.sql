DROP VIEW IF EXISTS tblChild11_{build_id}_{build};
CREATE VIEW tblChild11_{build_id}_{build}
AS
SELECT
I.MODEL_CD,
IA.MZB_INDIV_ID,
IA.MZB_HOUSEHOLD_ID,
I.DATE_KEY,
I.DECILE,
I.SCORE,
I.PROBABILITY,
I.CREATED_DT,
I.CREATED_ID,
I.UPDATED_DT,
I.UPDATED_ID,
I.SOURCE_CD FROM public.TBMZB_MODEL_SCORES I left outer join public.tbmzb_individual_merge_fix mrgd  on i.mzb_indiv_id=mrgd.MRGD_MZB_INDIV_ID join public.TBMZB_INDIVIDUAL_A IA on nvl(mrgd.SURV_MZB_INDIV_ID,I.mzb_indiv_id)=IA.mzb_indiv_id
with no schema binding;