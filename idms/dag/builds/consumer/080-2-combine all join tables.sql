DROP TABLE IF EXISTS IG_Consumer_Child_Combined;
CREATE TABLE IG_Consumer_Child_Combined
DISTKEY(ID)
SORTKEY(ID)
AS 
SELECT a.id, 
a.cstatecode, 
a.ccountycode, 
a.ccounty,
g.cellphone,
g.cellphone_areacode,
g.cellphone_number,
g.cellphone_cordcutter,
g.cellphone_prepaid_indicator,
g.cellphone_donotcallflag,
g.cellphone_verified_code,
g.cellphone_activitystatus,
g.cellphone_filterflag,
g.cellphone_filterreason,
g.cellphone_isdqi,
g.cellphone_matchlevel,
g.cellphone_matchscore,
g.cellphone_modifiedscore,
g.cellphone_individualmatch,
c.segmentcode_a,
c.segmentcode_f,
c.segmentcode_e,
c.segmentcode_i,
c.segmentcode_g,
c.segmentcode_h,
c.segmentcode_j,
d.mailorderbuyer_01,
d.mailorderbuyer_02,
d.mailorderbuyer_03,
d.mailorderbuyer_04,
d.mailorderbuyer_05,
d.mailorderbuyer_06,
d.mailorderbuyer_07,
d.mailorderbuyer_08,
d.mailorderbuyer_09,
d.mailorderbuyer_10,
d.mailorderbuyer_11,
d.mailorderbuyer_12,
d.mailorderbuyer_13,
d.mailorderbuyer_14,
d.mailorderbuyer_15,
d.mailorderbuyer_16,
d.mailorderbuyer_18,
e.city,
f.cphone,
h.nielsen_county_rank_description,
h.income_description,
h.age_code_description,
h.home_equity_estimate_description,
h.home_sale_price_code_description,
h.home_value_code_description,
h.own_rent_indicator_description,
h.expendable_income_rank_description,
h.mortgage_amount_description,
h.mortgage_loan_type_description,
h.loan_to_value_ratio_ltv_description
FROM IG_CONSUMER_Countycode a
LEFT JOIN IG_CONSUMER_Cellphone g
ON a.id = g.id
LEFT JOIN SEGMENTCODE c
ON A.id = c.id
LEFT JOIN MailOrderBuyer d
ON a.id = d.id
LEFT JOIN IG_CONSUMER_NEW_PreferredCity E 
ON a.id = e.id
LEFT JOIN IG_CONSUMER_EXCLUDE_DONOTCALLFLAG f 
ON a.id = f.id
LEFT JOIN IG_CONSUMER_NEW_DDValues h 
ON a.id = h.id
;
