/*

CREATE VIEW AND CHILD TABLES.
*/

DROP TABLE IF EXISTS {maintable_name} CASCADE;
ALTER TABLE {table-businessemail-new} RENAME TO {maintable_name};


DROP VIEW IF EXISTS {view-dba-business-table1};
CREATE VIEW {view-dba-business-table1} AS
SELECT
	city,
	state_with_county_code_lrfs AS county_code,
	state_with_county_code_lrfs,
	LTRIM(RTRIM([state]))+LTRIM(RTRIM(city)) AS st_city,
	id,
	firstname,
	lastname,
	email_address_lrfs,
	primary_sic_code_lrfs,
	a4_digit_primary_sic_lrfs,
	a4_digit_secondary_sic_lrfs,
	a2_digit_primary_sic_lrfs,
	franchise_code_lrfs,
	secondary_sic2_lrfs,
	secondary_sic3_lrfs,
	secondary_sic4_lrfs,
	CASE WHEN website_lrfs <> '' THEN 'Y' ELSE 'N' END AS website_lrfs,
	company_name_lrfs,
	business_credit_score_code_lrfs,
	employee_size_code_lrfs,
	gender_lrfs,
	number_of_pc_lrfs,
	office_size_lrfs,
	professional_title_code,
	sales_volume_code_lrfs,
	state,
	title_lrfs,
	zip,
	zip AS zipradius,
	cbsa_lrfs AS cbsa_code,
	public_private_code_lrfs,
	md5saltedhash,
	vendor_id_lrfs,
	'B' AS databasename,
	business_status_code_lrfs,
	cdomain
FROM  {maintable_name}
WHERE cinclude = 'Y';

DROP VIEW IF EXISTS tblChild1_{build_id}_{build};
CREATE VIEW tblChild1_{build_id}_{build} AS 
	SELECT SIC_CODE AS PRIMARY_SIC_CODE_LRFS,
		   SIC_DESCRIPTION 
	FROM PUBLIC.SIC_CODE_6DIGITS_DESCRIPTIONS WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild2_{build_id}_{build};
CREATE VIEW tblChild2_{build_id}_{build} AS 
	SELECT SIC_CODE AS SECONDARY_SIC2_LRFS,
	 	   SIC_DESCRIPTION AS SIC_DESCRIPTION1 
	FROM PUBLIC.SIC_CODE_6DIGITS_DESCRIPTIONS WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild3_{build_id}_{build};
CREATE VIEW tblChild3_{build_id}_{build} AS 
	SELECT SIC_CODE AS SECONDARY_SIC3_LRFS,
	 	   SIC_DESCRIPTION AS SIC_DESCRIPTION2 
	FROM PUBLIC.SIC_CODE_6DIGITS_DESCRIPTIONS WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild4_{build_id}_{build};
CREATE VIEW tblChild4_{build_id}_{build} AS 
	SELECT SIC_CODE AS SECONDARY_SIC4_LRFS,
	 	   SIC_DESCRIPTION AS SIC_DESCRIPTION3 
	FROM PUBLIC.SIC_CODE_6DIGITS_DESCRIPTIONS WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild5_{build_id}_{build};
CREATE VIEW tblChild5_{build_id}_{build} AS 
	SELECT SIC_CODE AS SECONDARY_SIC_CODE_LRFS,
	 	   SIC_DESCRIPTION AS SIC_DESCRIPTION4 
	FROM PUBLIC.SIC_CODE_6DIGITS_DESCRIPTIONS WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild6_{build_id}_{build};
CREATE VIEW tblChild6_{build_id}_{build} AS 
	SELECT CCODE AS SALES_VOLUME_CODE_LRFS,
	 	   CDESCRIPTION AS SALES_VOLUME_DESCRIPTION 
	FROM PUBLIC.SALES_VOLUME_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild7_{build_id}_{build};
CREATE VIEW tblChild7_{build_id}_{build} AS 
	SELECT CCODE AS BUSINESS_STATUS_CODE_LRFS,
	 	   CDESCRIPTION AS BUSINESS_STATUS_CODE_DESCRIPTION 
	FROM PUBLIC.BUSINESS_STATUS_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild8_{build_id}_{build};
CREATE VIEW tblChild8_{build_id}_{build} AS 
	SELECT CCODE AS AD_SIZE_LRFS,
	 	   CDESCRIPTION AS AD_SIZE_DESCRIPTION 
	FROM PUBLIC.AD_SIZE_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild9_{build_id}_{build};
CREATE VIEW tblChild9_{build_id}_{build} AS 
	SELECT CCODE AS MULTI_TENANT_CODE_LRFS,
	 	   CDESCRIPTION AS MULTI_TENANT_CODE_DESCRIPTION 
	FROM PUBLIC.MULTI_TENANT_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild10_{build_id}_{build};
CREATE VIEW tblChild10_{build_id}_{build} AS 
	SELECT CCODE AS PUBLIC_PRIVATE_CODE_LRFS,
	 	   CDESCRIPTION AS PUBLIC_PRIVATE_CODE_DESCRIPTION 
	FROM PUBLIC.PUBLIC_PRIVATE_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild11_{build_id}_{build};
CREATE VIEW tblChild11_{build_id}_{build} AS 
	SELECT CCODE AS TITLE_LRFS,
	 	   CDESCRIPTION AS TITLE_DESCRIPTION 
	FROM PUBLIC.TITLE_CODE_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild12_{build_id}_{build};
CREATE VIEW tblChild12_{build_id}_{build} AS 
	SELECT CCODE AS STATE,
	 	   CDESCRIPTION AS STATE_DESCRIPTION 
	FROM PUBLIC.STATE_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild13_{build_id}_{build};
CREATE VIEW tblChild13_{build_id}_{build} AS 
	SELECT CCODE AS EMPLOYEE_SIZE_CODE_LRFS,
	 	   CDESCRIPTION AS EMPLOYEESIZE_DESCRIPTION 
	FROM PUBLIC.EMPLOYEESIZE_DECODE WITH NO SCHEMA BINDING;

DROP VIEW IF EXISTS tblChild14_{build_id}_{build};
CREATE VIEW tblChild14_{build_id}_{build} AS 
	SELECT CCODE AS STATE_WITH_COUNTY_CODE_LRFS,
	 	   CDESCRIPTION AS COUNTYSTATE_DESCRIPTION 
	FROM PUBLIC.COUNTYSTATE_DECODE WITH NO SCHEMA BINDING;