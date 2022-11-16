DROP VIEW IF EXISTS tblChild2_{build_id}_{build};
CREATE VIEW tblChild2_{build_id}_{build}
AS
SELECT
IA.MZB_INDIV_ID,
IA.MZB_HOUSEHOLD_ID,
IA.MZB_ADDRESS_ID,
I.SOURCE_RECORD_ID,
I.MEMBER_ID,
I.PLAN_EFFECTIVE_DATE,
I.HIOS_PLAN_VARIANT_ID,
I.RECIPIENT_ID,
I.RECIPIENT_TYPE,
I.MAILING_ID,
I.REPORT_ID,
I.CAMPAIGN_ID,
I.EMAIL,
I.EVENT_TYPE,
I.EVENT_TIMESTAMP,
I.BODY_TYPE,
I.CONTENT_ID,
I.CLICK_NAME,
I.URL,
I.CONVERSION_ACTION,
I.CONVERSION_DETAIL,
I.CONVERSION_AMOUNT,
I.SUPPRESSION_REASON,
I.MAILING_NAME,
I.MAILING_SUBJECT,
I.CREATED_ID,
I.CREATED_DT,
I.UPDATED_DT,
I.UPDATED_ID,
I.NEWSLETTER_OPTOUT,
I.MARKETING_OPTOUT,
I.SOURCE_CD,
I.LOAD_DT,
I.JOB_ID,
I.ACCOUNT_ID,
I.ACCOUNT_USER_ID,
I.FROM_NAME,
I.FROM_EMAIL,
I.SCHED_TIME,
I.PICKUP_TIME,
I.DELIVERED_TIME,
I.EVENT_ID,
I.IS_MULTIPART,
I.JOB_TYPE,
I.JOB_STATUS,
I.MODIFIED_BY,
I.MODIFIED_DATE,
I.IS_WRAPPED,
I.TEST_EMAIL_ADDR,
I.CATEGORY,
I.BCC_EMAIL,
I.ORIGINAL_SCHED_TIME,
I.CREATED_DATE,
I.CHARACTER_SET,
I.IP_ADDRESS,
I.SF_TOTAL_SUBSCRIBER_COUNT,
I.SF_ERROR_SUBSCRIBER_COUNT,
I.SEND_TYPE,
I.DYNAMIC_EMAIL_SUBJECT,
I.SUPPRESS_TRACKING,
I.SEND_CLASSIFICATION_TYPE,
I.SEND_CLASSIFICATION,
I.RESOLVE_LINK_WITH_CURRENT_DATA,
I.EMAIL_SEND_DEFINITION,
I.DEDUPLICATE_BY_EMAIL,
I.TRIGGERED_SEND_DEFNTN_OBJ_ID,
I.TRIGGERED_SEND_CUSTOMER_KEY,
I.BOUNCE_CATEGORY_ID,
I.BOUNCE_CATEGORY,
I.BOUNCE_SUBCATEGORY_ID,
I.BOUNCE_SUBCATEGORY,
I.BOUNCE_TYPE_ID,
I.BOUNCE_TYPE,
I.SMTP_BOUNCE_REASON,
I.SMTP_MESSAGE,
I.SMTP_CODE,
I.DOMAIN,
I.LINK_CONTENT,
I.IS_UNIQUE,
I.SUBSCRIBER_ID,
I.BOUNCE_COUNT,
I.DATE_JOINED,
I.DATE_UNDELIVERABLE,
I.DATE_UNSUBSCRIBED,
I.LIST_ID,
I.DIVISION,
I.PLAN_SPONSOR_ID,
I.PLAN_SPONSOR_NAME,
I.WORK_EMAIL,
I.EMAIL_CREATE_DATE,
I.CELL_PIECE_CD,
I.CELL_TREATMENT_DESC,
I.FOLLOWUP_EMAIL_FLAG,
I.TEST_FLAG,
I.FILLER_5,
I.FILLER_6,
I.FILLER_7,
I.FILLER_8,
I.FILLER_9,
I.FILLER_10,
I.SESSION_ID,
I.VISITOR_ID,
I.ADOBEWEBATTRIBUTE1,
I.CAMPAIGN_DESCRIPTION,
I.ADOBEWEBATTRIBUTE2,
I.FILLER_16,
I.FILLER_17,
I.FILLER_18,
I.FILLER_19,
I.FILLER_20
FROM public.TBMZB_EMAIL_INTERACTIONS I left outer join public.tbmzb_individual_merge_fix mrgd  
on i.mzb_indiv_id=mrgd.MRGD_MZB_INDIV_ID join public.TBMZB_INDIVIDUAL_A IA 
on nvl(mrgd.SURV_MZB_INDIV_ID,I.mzb_indiv_id)=IA.mzb_indiv_id
with no schema binding;