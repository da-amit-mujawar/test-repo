-- Delete and re-create table
DROP TABLE IF EXISTS interna.enterprise_sales_delta;
CREATE TABLE interna.enterprise_sales_delta
(
    Sub_BU VARCHAR(65535)
    ,Standard_Division VARCHAR(65535)
    ,Division VARCHAR(65535)
    ,Standard_Name VARCHAR(65535)
    ,Product VARCHAR(65535)
    ,Standard_Product VARCHAR(65535)
    ,GM VARCHAR(65535)
    ,AD_Sales_Rep VARCHAR(65535)
    ,Jan_22 FLOAT4
    ,Feb_22 FLOAT4
    ,Mar_22 FLOAT4
    ,Apr_22 FLOAT4
    ,May_22 FLOAT4
    ,Jun_22 FLOAT4
    ,Jul_22 FLOAT4
    ,Aug_22 FLOAT4
    ,Sep_22 FLOAT4
    ,Oct_22 FLOAT4
    ,Nov_22 FLOAT4
    ,Dec_22 FLOAT4
    ,Jan_6_6 FLOAT4
    ,Feb_6_6 FLOAT4
    ,Mar_6_6 FLOAT4
    ,Apr_6_6 FLOAT4
    ,May_6_6 FLOAT4
    ,Jun_6_6 FLOAT4
    ,Jul_6_6 FLOAT4
    ,Aug_6_6 FLOAT4
    ,Sep_6_6 FLOAT4
    ,Oct_6_6 FLOAT4
    ,Nov_6_6 FLOAT4
    ,Dec_6_6 FLOAT4
    ,Jan_4_8 FLOAT4
    ,Feb_4_8 FLOAT4
    ,Mar_4_8 FLOAT4
    ,Apr_4_8 FLOAT4
    ,May_4_8 FLOAT4
    ,Jun_4_8 FLOAT4
    ,Jul_4_8 FLOAT4
    ,Aug_4_8 FLOAT4
    ,Sep_4_8 FLOAT4
    ,Oct_4_8 FLOAT4
    ,Nov_4_8 FLOAT4
    ,Dec_4_8 FLOAT4
    ,Jan_22_Bgt FLOAT4 
    ,Feb_22_Bgt FLOAT4
    ,Mar_22_Bgt FLOAT4
    ,Apr_22_Bgt FLOAT4
    ,May_22_Bgt FLOAT4
    ,Jun_22_Bgt FLOAT4
    ,Jul_22_Bgt FLOAT4
    ,Aug_22_Bgt FLOAT4
    ,Sep_22_Bgt FLOAT4
    ,Oct_22_Bgt FLOAT4
    ,Nov_22_Bgt FLOAT4
    ,Dec_22_Bgt FLOAT4
    ,Jan_21 FLOAT4
    ,Feb_21 FLOAT4
    ,Mar_21 FLOAT4
    ,Apr_21 FLOAT4
    ,May_21 FLOAT4
    ,Jun_21 FLOAT4
    ,Jul_21 FLOAT4
    ,Aug_21 FLOAT4
    ,Sep_21 FLOAT4
    ,Oct_21 FLOAT4
    ,Nov_21 FLOAT4
    ,Dec_21 FLOAT4
    ,CM_Actual FLOAT4
    ,CM_6_6 FLOAT4
    ,CM_4_8 FLOAT4
    ,CM_Budget FLOAT4
    ,CM_PY FLOAT4
    ,CM_vs_6_6 FLOAT4
    ,CM_vs_4_8 FLOAT4
    ,CM_vs_Budget FLOAT4
    ,CM_vs_PY FLOAT4
    ,YTD_Actual FLOAT4
    ,YTD_6_6 FLOAT4
    ,YTD_4_8 FLOAT4
    ,YTD_Budget FLOAT4
    ,YTD_PY FLOAT4
    ,YTD_vs_6_6 FLOAT4
    ,YTD_vs_4_8 FLOAT4
    ,YTD_vs_Budget FLOAT4
    ,YTD_vs_PY FLOAT4
    ,FY_Fcst FLOAT4
    ,FY_6_6 FLOAT4
    ,FY_4_8 FLOAT4
    ,FY_Budget FLOAT4
    ,FY_PY FLOAT4
    ,FY_vs_6_6 FLOAT4
    ,FY_vs_4_8 FLOAT4
    ,FY_vs_Budget FLOAT4
    ,FY_vs_PY FLOAT4
    ,FY_Category VARCHAR(65535)
    ,FY_Budget_Category VARCHAR(65535)
    ,YTD_Category VARCHAR(65535)
    ,YTD_Budget_Category VARCHAR(65535)
    ,CM_Category___4_8 VARCHAR(65535)
    ,CM_Category___Budget VARCHAR(65535)
    ,Budget___CM VARCHAR(65535)
    ,File_Version VARCHAR(65535)
);


--Copy data from s3 to redshift table
COPY interna.enterprise_sales_delta
FROM 's3://{bucket_name}/{file_key}'
IAM_ROLE '{iam}'
CSV QUOTE AS '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1;

-- Drop file_version column and Update it to today's date
ALTER TABLE interna.enterprise_sales_delta
DROP COLUMN File_Version;
ALTER TABLE interna.enterprise_sales_delta
ADD COLUMN File_Version DATE 
DEFAULT '{yyyy}{mm}{dd}';

--Unload sample output
UNLOAD ('SELECT a.* FROM (SELECT * FROM interna.enterprise_sales_delta LIMIT 100)a')
TO 's3://{s3-internal}/Reports/enterprise_sales_data_output_{yyyy}{mm}{dd}'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV DELIMITER AS ','
ALLOWOVERWRITE
HEADER
PARALLEL OFF
;