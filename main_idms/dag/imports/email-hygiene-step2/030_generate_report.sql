-- Creating report from processed file
DROP TABLE IF EXISTS ehyg_{dbid}_report1;
CREATE TABLE ehyg_{dbid}_report1 (Engaged_Flag CHAR(5), Count BIGINT);

INSERT    INTO ehyg_{dbid}_report1
SELECT    Engaged_Flag,
          COUNT(*) AS COUNT
FROM      ehyg_{dbid}_step2 
GROUP BY  Engaged_Flag
ORDER BY  Engaged_Flag;

---------------------------------------------------
DROP TABLE IF EXISTS ehyg_{dbid}_report2;
CREATE TABLE ehyg_{dbid}_report2 (EO_Validation_StatusID INT, EO_Validation_Status VARCHAR(150), Count BIGINT);

INSERT    INTO ehyg_{dbid}_report2
SELECT    CAST(EO_Validation_StatusID AS INT),
          EO_Validation_Status,
          COUNT(*) AS COUNT
FROM      ehyg_{dbid}_step2 
GROUP BY  EO_Validation_StatusID,
          EO_Validation_Status
ORDER BY  CAST(EO_Validation_StatusID AS INT),
          EO_Validation_Status;
---------------------------------------------------
DROP TABLE IF EXISTS ehyg_{dbid}_report3;
CREATE TABLE ehyg_{dbid}_report3 (EO_EmailDomainGroupID INT, EO_EmailDomainGroup VARCHAR(150), Count BIGINT);

INSERT    INTO ehyg_{dbid}_report3
SELECT    CAST(EO_EmailDomainGroupID AS INT),
          EO_EmailDomainGroup,
          COUNT(*) AS COUNT
FROM      ehyg_{dbid}_step2 
GROUP BY  EO_EmailDomainGroupID,
          EO_EmailDomainGroup
ORDER BY  CAST(EO_EmailDomainGroupID AS INT),
          EO_EmailDomainGroup;
---------------------------------------------------
DROP TABLE IF EXISTS ehyg_{dbid}_report4;
CREATE TABLE ehyg_{dbid}_report4 (Ehygiene_Code INT, Count BIGINT);

INSERT    INTO ehyg_{dbid}_report4
SELECT    CAST(Ehygiene_Code AS INT),
          COUNT(*) AS COUNT
FROM      ehyg_{dbid}_step2 
GROUP BY  Ehygiene_Code
ORDER BY  CAST(Ehygiene_Code AS INT);
---------------------------------------------------
DROP TABLE IF EXISTS ehyg_{dbid}_report5;
CREATE TABLE ehyg_{dbid}_report5 (IDMS_Ehyg_Code INT, Count BIGINT);

INSERT    INTO ehyg_{dbid}_report5
SELECT    CAST(Ehygiene_Code AS INT),
          COUNT(*) AS COUNT
FROM      ehyg_{dbid}_IDMS
GROUP BY  Ehygiene_Code
ORDER BY  CAST(Ehygiene_Code AS INT);
---------------------------------------------------
CREATE TABLE ehyg_{dbid}_report6 (Type VARCHAR(200), Count float);

INSERT    INTO ehyg_{dbid}_report6
SELECT    'Input', cast(COUNT(*) as float)
FROM      ehyg_{dbid}_step2 ;

INSERT    INTO ehyg_{dbid}_report6
SELECT    'Suppressions', cast (COUNT(*) as float)
FROM      ehyg_{dbid}_Suppressions;

INSERT    INTO ehyg_{dbid}_report6
select    'Suppressions_Percent', 
          round((select count from ehyg_{dbid}_report6 where type='Suppressions')*100.0/
          (select count from ehyg_{dbid}_report6 where type='Input'), 3);
