DROP VIEW IF EXISTS tblChild3_{build_id}_{build};
CREATE VIEW tblChild3_{build_id}_{build}
AS
SELECT
IA.MZB_INDIV_ID,
IA.MZB_HOUSEHOLD_ID,
IA.MZB_ADDRESS_ID,
I.SOURCE_RECORD_ID,
I.DPBC,
I.CRRT,
I.FIPS,
I.COUNTY,
I.CONG_DISTRICT,
I.SOURCE_CD,
I.CREATED_DT,
I.CREATED_ID,
I.UPDATED_DT,
I.UPDATED_ID,
I.LOAD_DT,
SOURCE FROM public.TBMZB_FEDERAL_EMPLOYEES I left outer join public.tbmzb_individual_merge_fix mrgd  on i.mzb_indiv_id=mrgd.MRGD_MZB_INDIV_ID join 
public.TBMZB_INDIVIDUAL_A IA on nvl(mrgd.SURV_MZB_INDIV_ID,I.mzb_indiv_id)=IA.mzb_indiv_id
with no schema binding;