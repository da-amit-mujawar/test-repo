DROP TABLE IF EXISTS NoSuchTable;


-- Get the DD data in staging table  from sql_tblDDDescriptions
DROP TABLE IF EXISTS IG_CONSUMER_NEW_DDValues_Load;

CREATE TABLE IG_CONSUMER_NEW_DDValues_Load 
AS 
SELECT
    LTRIM(RTRIM(UPPER(cfieldname))) AS cfieldname, 
    LTRIM(RTRIM(UPPER(cvalue))) AS cvalue, 
    LTRIM(RTRIM(cdescription)) AS cdescription 
FROM sql_tblDDDescriptions 
WHERE databaseid =1267 AND 
      LTRIM(RTRIM(cfieldname))IN ('NIELSEN_COUNTY_RANK',
                                    'INCOME',
                                    'AGE_CODE',
                                    'HOME_EQUITY_ESTIMATE',
                                    'HOME_SALE_PRICE_CODE',
                                    'HOME_VALUE_CODE',
                                    'OWN_RENT_INDICATOR',
                                    'EXPENDABLE_INCOME_RANK',
                                    'MORTGAGE_AMOUNT',
                                    'MORTGAGE_LOAN_TYPE',
                                    'LOAN_TO_VALUE_RATIO_LTV')AND 
      (LTRIM(RTRIM(UPPER(cvalue))) <> '' OR LTRIM(RTRIM(cdescription)) <>  '');


DROP TABLE IF EXISTS IG_CONSUMER_NEW_DDValues_Clean;
CREATE TABLE IG_CONSUMER_NEW_DDValues_Clean
DISTKEY (ID)
SORTKEY(ID)
AS
SELECT id, 
       NVL(B1.cdescription,'') AS nielsen_county_rank_description, 
       NVL(B2.cdescription,'') AS income_description,
       NVL(B3.cdescription,'')  AS age_code_description,
       NVL(B4.cdescription,'') AS home_equity_estimate_description,
       NVL(B5.cdescription,'') AS home_sale_price_code_description,
       NVL(B6.cdescription,'') AS home_value_code_description,
       NVL(B7.cdescription,'') AS own_rent_indicator_description,
       NVL(B8.cdescription,'') AS expendable_income_rank_description,
       NVL(B9.cdescription,'') AS mortgage_amount_description,
       NVL(B10.cdescription,'') AS mortgage_loan_type_description,
       NVL(B11.cdescription,'') AS loan_to_value_ratio_ltv_description
 FROM  {new-load-table} A 
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B1  ON NVL(A.nielsen_county_rank,'')  = NVL(B1.cvalue,'') AND B1.cfieldname ='NIELSEN_COUNTY_RANK'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B2  ON NVL(A.income,'')  = NVL(B2.cvalue,'') AND B2.cfieldname ='INCOME'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B3  ON NVL(A.age_code,'')  = NVL(B3.cvalue,'') AND B3.cfieldname ='AGE_CODE'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B4  ON NVL(A.home_equity_estimate,'')  = NVL(B4.cvalue,'') AND B4.cfieldname ='HOME_EQUITY_ESTIMATE'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B5  ON NVL(A.home_sale_price_code,'')  = NVL(B5.cvalue,'') AND B5.cfieldname ='HOME_SALE_PRICE_CODE'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B6  ON NVL(A.home_value_code,'')  = NVL(B6.cvalue,'') AND B6.cfieldname ='HOME_VALUE_CODE'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B7  ON NVL(A.own_rent_indicator,'')  = NVL(B7.cvalue,'') AND B7.cfieldname ='OWN_RENT_INDICATOR'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B8  ON NVL(A.expendable_income_rank,'')  = NVL(B8.cvalue,'') AND B8.cfieldname ='EXPENDABLE_INCOME_RANK'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B9  ON NVL(A.mortgage_amount,'')  = NVL(B9.cvalue,'') AND B9.cfieldname ='MORTGAGE_AMOUNT'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B10  ON NVL(A.mortgage_loan_type,'')  = NVL(B10.cvalue,'') AND B10.cfieldname ='MORTGAGE_LOAN_TYPE'
 LEFT JOIN IG_CONSUMER_NEW_DDValues_Load B11  ON NVL(A.loan_to_value_ratio_ltv,'')  = NVL(B11.cvalue,'') AND B11.cfieldname ='LOAN_TO_VALUE_RATIO_LTV'
 ;


DROP TABLE IF EXISTS IG_CONSUMER_NEW_DDValues;
CREATE TABLE IG_CONSUMER_NEW_DDValues 
DISTKEY (ID)
SORTKEY(ID)
AS 
SELECT id AS id, 
    MIN(nielsen_county_rank_description) AS nielsen_county_rank_description, 
    MIN(income_description) AS income_description,
    MIN(age_code_description) AS age_code_description ,
    MIN(home_equity_estimate_description) AS home_equity_estimate_description,
    MIN(home_sale_price_code_description) AS home_sale_price_code_description,
    MIN(home_value_code_description) AS home_value_code_description,
    MIN(own_rent_indicator_description) AS own_rent_indicator_description,
    MIN(expendable_income_rank_description) AS expendable_income_rank_description,
    MIN(mortgage_amount_description) AS mortgage_amount_description ,
    MIN(mortgage_loan_type_description) AS mortgage_loan_type_description,
    MIN(loan_to_value_ratio_ltv_description) AS loan_to_value_ratio_ltv_description
FROM IG_CONSUMER_NEW_DDValues_Clean DDValues
GROUP BY ID
;


