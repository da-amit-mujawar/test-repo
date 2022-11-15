DROP VIEW IF EXISTS tblChild5_{build_id}_{build};
CREATE VIEW tblChild5_{build_id}_{build}
AS
SELECT
IA.MZB_INDIV_ID,
I.EMAIL_ADDRESS,
I.BRAND_CD,
I.CREATED_DT,
I.LAST_UPDATED_DT,
I.AETNA_SOURCE,
I.EMAIL_VALID_IND,
I.DO_NOT_EMAIL_IND,
I.EMAIL_BOUNCE_IND,
I.EMAIL_HYGIENE_CD,
I.NEWSLETTER_SUBSCRIPTION,
I.OK_TO_EMAIL_IND,
I.GLOBAL_SUPPRESSION_IND 
FROM public.TBMZB_INDIVIDUAL_EMAIL I left outer join public.tbmzb_individual_merge_fix mrgd  
on i.mzb_indiv_id=mrgd.MRGD_MZB_INDIV_ID join public.TBMZB_INDIVIDUAL_A IA on nvl(mrgd.SURV_MZB_INDIV_ID,I.mzb_indiv_id)=IA.mzb_indiv_id
with no schema binding;