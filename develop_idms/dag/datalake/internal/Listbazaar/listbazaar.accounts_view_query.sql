USE [Listbazaar]
GO

/****** Object:  View [dbo].[View_Accounts_DSG]    Script Date: 5/12/2021 6:19:50 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE view [dbo].[View_Accounts_DSG] 
as
select
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),account_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),x_account_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),account_desc),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),password),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),m_maiden_name),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),last_name),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),first_name),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),company),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),address),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),city),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),state),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),zip),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),country),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),phone_no),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),fax_no),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),email),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),created_date),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),init_registrar),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),status_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),notes),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),company_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),media_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),segment_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),salerep_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),abi_no),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),url),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),advertiser_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),passwd_hint_question),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),passwd_hint_answer),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),territory_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),email_status),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),term_accepted),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),vendor_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),service_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),customer_ref),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),sec_phone_no),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),match_flag),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),archive_flag),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),customer_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),sf_leadcontact_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),phone_ext),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),last_modified_date),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),last_sf_sync_date),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),industry),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),no_employees),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),think_login_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),think_account_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),think_contact_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),NewSgAccountId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),NewSgUserId),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),interested_prospects),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),job_title),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),attach_order_file_flag),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),bel_approved_flag),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),sales_division_id),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),email_optout_flag),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"|' +
'"' + RTRIM(LTRIM(Replace(Replace(Replace(Replace(replace(replace(replace(COALESCE(CONVERT(VARCHAR(MAX),express_checkout_flag),''),char(13)+char(10),''),char(44),''),char(36),''),char(10),''),char(13),''),char(34),''),'|',''))) + '"'
as ViewData from Accounts




GO


