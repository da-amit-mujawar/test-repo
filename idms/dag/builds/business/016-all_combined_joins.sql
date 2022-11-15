DROP TABLE IF EXISTS All_Business_Combined;
CREATE TABLE All_Business_Combined
DISTKEY(id)
SORTKEY(id)
AS
select
    a.id AS id,
    a.application_a AS application_a,
    a.application_b AS application_b,
    a.application_c AS application_c,
    a.application_e AS application_e,
    a.application_f AS application_f,
    a.application_g AS application_g,
    a.application_h AS application_h,
    a.application_i AS application_i,
    a.application_j AS application_j,
    a.application_k AS application_k,
    a.application_l AS application_l,
    a.application_m AS application_m,
    a.application_n AS application_n,
    a.application_p AS application_p,
    b.segmentcode_1 AS segmentcode_1,
    b.segmentcode_2 AS segmentcode_2,
    b.segmentcode_3 AS segmentcode_3,
    b.segmentcode_4 AS segmentcode_4,
    b.segmentcode_5 AS segmentcode_5,
    b.segmentcode_6 AS segmentcode_6,
    b.segmentcode_7 AS segmentcode_7,
    b.segmentcode_8 AS segmentcode_8,
    b.segmentcode_9 AS segmentcode_9,
    b.segmentcode_a AS segmentcode_a,
    b.segmentcode_f AS segmentcode_f,
    b.segmentcode_h AS segmentcode_h,
    b.segmentcode_i AS segmentcode_i,
    c.state_description AS state_description,
    c.ethnicity_description AS ethnicity_description,
    c.stockexchange_description AS stockexchange_description,
    c.phonecallstatus_description AS phonecallstatus_description,
    c.businesscreditalphascore_description AS businesscreditalphascore_description,
    c.fleetsize_description AS fleetsize_description,
    d.accounting_expense AS accounting_expense,
    d.advertising_expense AS advertising_expense,
    d.charitable_dontations_expense AS charitable_dontations_expense,
    d.contract_labor_expense AS contract_labor_expense,
    d.insurance_expense AS insurance_expense,
    d.legal_expense AS legal_expense,
    d.license_fees_taxes AS license_fees_taxes,
    d.maintenance_repair AS maintenance_repair,
    d.office_equipment_expense AS office_equipment_expense,
    d.packagecontainer_expense AS packagecontainer_expense,
    d.payroll_expense AS payroll_expense,
    d.purchased_print_expense AS purchased_print_expense,
    d.purchase_mgmtadmin_svc_expense AS purchase_mgmtadmin_svc_expense,
    d.rent_leasing_expense AS rent_leasing_expense,
    d.technology_expense AS technology_expense,
    d.telecommunications_expense AS telecommunications_expense,
    d.transportationexpensecode AS transportationexpensecode,
    d.utilities_expense AS utilities_expense,
    d.salutation AS salutation,
    d.acl_groupcode AS acl_groupcode,
    d.acl_denominationcode AS acl_denominationcode,
    d.dmacode AS dmacode,
    d.proftitle AS proftitle,
    d.addresstype AS addresstype,
    d.ownorlease AS ownorlease,
    d.ami_boardcertifiedindicator AS ami_boardcertifiedindicator,
    d.ami_primaryspecialty AS ami_primaryspecialty,
    d.ami_secondaryspecialty AS ami_secondaryspecialty,
    d.ami_professionaldegreecode AS ami_professionaldegreecode,
    d.ami_lic_stateoflicense AS ami_lic_stateoflicense,
    d.ami_prescriptionsperwk AS ami_prescriptionsperwk,
    d.creditratingcode AS creditratingcode,
    d.phone_number_type AS phone_number_type,
    d.district_description AS district_description,
    d.credit_rating_description AS credit_rating_description,
    d.title_description AS title_description,
    d.gender_description AS gender_description,
    d.cma_description AS cma_description,
    e.franchise_code_1_description AS  franchise_code_1_description,
    e.franchise_code_2_description AS franchise_code_2_description,
    e.franchise_code_3_description AS franchise_code_3_description,
    e.franchise_code_4_description AS franchise_code_4_description,
    e.franchise_code_5_description AS franchise_code_5_description,
    e.franchise_code_6_description AS franchise_code_6_description,
    f.hospitalname AS hospitalname, 
    f.hospitalnumber AS hospitalnumber,
    g.zipcode AS zipcode, 
    g.city AS city,
    h.csiccode AS csiccode, 
    h.cindustryspecificcode AS cindustryspecificcode, 
    --a.INDUSTRY_DESC AS INDUSTRY_DESC
    h.cindustrydesc AS cindustrydesc,
    j.primarysic2_desc AS primarysic2_desc,
    j.primarysic4_desc AS primarysic4_desc
FROM business_APPLICATION a
LEFT JOIN business_SEGMENTCODE b ON a.id=b.id
LEFT JOIN ddvalues_1_dddescriptions c ON a.id=c.id
LEFT JOIN ddvalues_2_dddescriptions d ON a.id=d.id
LEFT JOIN DDvalues3_franchise e ON a.id=e.id
LEFT JOIN HospitalName f ON a.id=f.id
LEFT JOIN PreferredCity g ON a.id=g.id
LEFT JOIN tmpIndustryCode_load h ON a.id=h.id 
LEFT JOIN business_tblSICCode j ON a.id=j.id;
