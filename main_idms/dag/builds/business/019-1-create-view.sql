--tblChild1
CREATE VIEW tblChild1_{build_id}_{build} 
AS 
SELECT SIC_Code AS PRIMARYSIC6, SIC_Description AS SIC_Description1 
FROM SIC_Code_6Digits_Descriptions;

--tblChild2
CREATE VIEW tblChild2_{build_id}_{build} 
AS 
SELECT SIC_Code AS SICCODE1, SIC_Description AS SIC_Description2 
FROM SIC_Code_6Digits_Descriptions;

--tblChild3
CREATE VIEW tblChild3_{build_id}_{build} 
AS 
SELECT SIC_Code AS SICCODE2, SIC_Description AS SIC_Description3 
FROM SIC_Code_6Digits_Descriptions;

--tblChild4
CREATE VIEW tblChild4_{build_id}_{build} 
AS 
SELECT SIC_Code AS SICCODE3, SIC_Description AS SIC_Description4 
FROM SIC_Code_6Digits_Descriptions;

--tblChild5
CREATE VIEW tblChild5_{build_id}_{build} 
AS 
SELECT SIC_Code AS SICCODE4, SIC_Description AS SIC_Description5 
FROM SIC_Code_6Digits_Descriptions;


--tblChild6
CREATE VIEW tblChild6_{build_id}_{build} 
AS 
SELECT cCode AS OUTPUTVOLUMECODE1,cDescription AS Sales_Volume_Corporate_Description 
FROM Sales_Volume_Decode;

--tblChild7
CREATE VIEW tblChild7_{build_id}_{build} 
AS 
SELECT cCode AS BUSINESSSTATUSCODE,cDescription AS Business_Status_Code_Description 
FROM Business_Status_Decode;

--tblChild8
CREATE VIEW tblChild8_{build_id}_{build} 
AS 
SELECT cCode AS ADSIZECODE, cDescription AS Ad_Size_Description 
FROM AD_Size_Decode;

--tblChild9
CREATE VIEW tblChild9_{build_id}_{build} 
AS 
SELECT cCode AS MULTITENANTCODE, cDescription AS Multi_Tenant_Code_Description 
FROM Multi_Tenant_Decode;

--tblChild10
CREATE VIEW tblChild10_{build_id}_{build} 
AS 
SELECT cCode AS PUBLIC_PRIVATE_CODE_Email,cDescription AS Public_Private_Code_Description 
FROM Public_Private_Decode;

--tblChild11
CREATE VIEW tblChild11_{build_id}_{build} 
AS 
SELECT cCode AS TITLECODE, cDescription AS Title_Description, cShortDescription 
FROM Title_Code_Decode;

--tblChild12
CREATE VIEW tblChild12_{build_id}_{build} 
AS 
SELECT cCode AS State, cDescription AS State_Description 
FROM State_Decode;

--tblChild13
CREATE VIEW tblChild13_{build_id}_{build} 
AS 
SELECT cCode AS EMPLOYEESIZECODE1, cDescription AS EmployeeSize_Corporate_Description 
FROM EmployeeSize_Decode;

--tblChild14
CREATE VIEW tblChild14_{build_id}_{build} 
AS 
SELECT cCode AS StateCOUNTYCODE, cDescription AS CountyState_Description 
FROM CountyState_Decode;

--tblChild15
CREATE VIEW tblChild15_{build_id}_{build} 
AS 
SELECT cCode AS OUTPUTVOLUMECODE2, cDescription AS Sales_Volume_Location_Description 
FROM Sales_Volume_Decode;

--tblChild16
CREATE VIEW tblChild16_{build_id}_{build} 
AS 
SELECT cCode AS EMPLOYEESIZECODE2, cDescription AS EmployeeSize_Location_Description 
FROM EmployeeSize_Decode;

--tblChild17
CREATE VIEW tblChild17_{build_id}_{build} 
AS 
SELECT NAICS8 AS NAICSCODE, Descriptions As NAICS8_Descriptions 
FROM EXCLUDE_NAICS8_Descriptions;

--tblChild18
CREATE VIEW tblChild18_{build_id}_{build}  
AS SELECT CBSACODE , CBTITLE 
FROM EXCLUDE_CBSA_Descriptions;

--tblChild19
CREATE VIEW tblChild19_{build_id}_{build}  
AS 
SELECT CSACODE,CSA_NAME 
FROM EXCLUDE_CSA_Descriptions;

--tblChild20
CREATE VIEW tblChild20_{build_id}_{build} 
AS 
SELECT cCode AS BANKASSETCODE, cDescription AS BANKASSETCODE_Description 
FROM EXCLUDE_BANKASSETCODE_Descriptions;

--tblChild21
CREATE VIEW tblChild21_{build_id}_{build} 
AS 
SELECT cCode AS COMPANYHOLDINGSTATUS, cDescription AS COMPANYHOLDINGSTATUS_Description 
FROM EXCLUDE_COMPANYHOLDINGSTATUS_Descriptions;

--tblChild22
CREATE VIEW tblChild22_{build_id}_{build} 
AS 
SELECT cCode AS IDENTIFICATIONCODE, cDescription AS IDENTIFICATIONCODE_Description 
FROM EXCLUDE_INDIVIDUALFIRM_Descriptions;

--tblChild23
CREATE VIEW tblChild23_{build_id}_{build} 
AS 
SELECT cCode AS OFFICESIZECODE,cDescription AS OFFICESIZECODE_Description 
FROM EXCLUDE_OFFICESIZECODE_Descriptions;

--tblChild24
CREATE VIEW tblChild24_{build_id}_{build} 
AS 
SELECT cCode AS POPCODE, cDescription AS POPCODE_Description 
FROM EXCLUDE_POPCODE_Descriptions;

--tblChild25
CREATE VIEW tblChild25_{build_id}_{build} 
AS 
SELECT cCode AS SQUAREFOOTAGE8, cDescription AS SQUAREFOOTAGE8_Description 
FROM Square_Footage_Descriptions;

--tblChild26
CREATE VIEW tblChild26_{build_id}_{build} 
AS 
SELECT cCode AS Inferred_Corporate_Employee_Range, cDescription AS Inferred_Corporate_Employee_Range_Description 
FROM EmployeeSize_Decode;

--tblChild27
CREATE VIEW tblChild27_{build_id}_{build} 
AS 
SELECT cCode AS Inferred_Corporate_Sales_Volume_Range, cDescription AS Inferred_Corporate_Sales_Volume_Range_Description 
FROM Sales_Volume_Decode;

--tblChild28
CREATE VIEW tblChild28_{build_id}_{build} 
AS 
SELECT cCode AS Functional_Area_Code, cDescription AS Functional_Area_Code_description 
FROM EXCLUDE_BusFunctionalAreaCode;

--tblChild29
CREATE VIEW tblChild29_{build_id}_{build} 
AS 
SELECT cCode AS Department_Code, cDescription AS Department_Code_description 
FROM EXCLUDE_BusDepartmentCode;

--tblChild30
CREATE VIEW tblChild30_{build_id}_{build} 
AS 
SELECT cCode AS Role_Code, cDescription AS Role_Code_description 
FROM EXCLUDE_BusRoleCode;

--tblChild31
CREATE VIEW tblChild31_{build_id}_{build} 
AS 
SELECT cCode AS Level_Code, cDescription AS Level_Code_description 
FROM EXCLUDE_BusLevelCode;

--tblChild35
DROP TABLE IF EXISTS tblChild35_{build_id}_{build};
CREATE TABLE tblChild35_{build_id}_{build}  
(
    ABINUMBER varchar(9) ,  
    EmpSize Varchar(1)
);

INSERT INTO tblChild35_{build_id}_{build} 
(
    ABINUMBER , 
    EmpSize
)
SELECT DISTINCT 
    ABINUMBER, 
    Inferred_Corporate_Employee_Range As EmpSize 
    FROM tblMain_{build_id}_{build} 
    Where ABINUMBER <>'' and 
    Inferred_Corporate_Employee_Range <>''
UNION
SELECT DISTINCT 
    ABINUMBER, 
    EMPLOYEESIZECODE2 As EmpSize 
    FROM tblMain_{build_id}_{build}  
    Where ABINUMBER <>'' and 
    EMPLOYEESIZECODE2 <>''
;
