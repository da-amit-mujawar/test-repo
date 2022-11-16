DROP TABLE IF EXISTS nosuchtable;
INSERT INTO exclude_vin_ntf_auto
SELECT vin,ntf_flag
FROM tblChild17_{pre_buildid}_{pre_build}
WHERE vin NOT IN (SELECT vin FROM exclude_vin_ntf_auto WHERE vin IS NOT NULL);


DROP TABLE IF EXISTS tblChild17_{build_id}_{build};   
CREATE TABLE tblChild17_{build_id}_{build} 
distkey (individual_id)
sortkey (individual_id)
AS
SELECT
       CASE WHEN individual_id<>'' THEN NVL(individual_id,'') ELSE NVL(individual_mc,'') END AS individual_id, 
        NVL(A.vin,'') as vin,
        NVL(shortvin,'') as shortvin,
        NVL(make,'') as make,
        NVL(model,'') as model,
        NVL(year,'') as year,
        NVL(odometer,'') as odometer,
        NVL(transaction_date,'') as transaction_date,
        NVL(transyyyymm,'') as transyyyymm,
        NVL(drive_type,'') as drive_type,
        NVL(fuel_type,'') as fuel_type,
        NVL(luxnon,'') as luxnon,
        NVL(bodytype,'') as bodytype,
        NVL(domimp,'') as domimp,
        NVL(SUBSTRING(vehtype,1,5),'') AS vehtype,
        NVL(transyear,'') as transyear,
        NVL(A.company_mc,'') AS oneperhhld,
        CASE WHEN A.vin IN (SELECT vin FROM exclude_vin_ntf_auto) THEN UPPER(NVL(B.ntf_flag,'')) ELSE UPPER(NVL(concat(to_char(add_months(getdate(),1), 'Mon'),extract(year from add_months(getdate(),1))),''))
            END AS ntf_flag,
        car_number,
        NVL(ah1_MATCH_code,'') as lems
    FROM auto_reformat_infutor A
    LEFT OUTER JOIN exclude_vin_ntf_auto B
    ON A.vin=B.vin
    LEFT OUTER JOIN(
                    SELECT matchcode, COUNT(DISTINCT vin) AS car_number 
                    FROM auto_reformat_infutor A GROUP BY A.matchcode
    ) C
    ON A.matchcode= C.matchcode;

--distkey and sort key would be individual_id
--external link should be part of consumer db.
--unload tblchild17
UNLOAD ('select * from tblChild17_{build_id}_{build} where individual_id in (select individual_id from tblChild17_{build_id}_{build} limit 100)')
TO 's3://idms-7933-internalfiles/Reports/auto_IDMS_extract'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV
CLEANPATH
HEADER
PARALLEL OFF
;

--external autodb-link dag
DROP TABLE IF EXISTS tblExternal42_191_201206_NEW;
CREATE TABLE tblExternal42_191_201206_NEW
(
    LEMS VARCHAR(18)  PRIMARY KEY encode raw,
    VIN VARCHAR(17) encode zstd,
    Make VARCHAR(30)encode bytedict,
    Auto_Model VARCHAR(30) encode zstd,
    Auto_Year VARCHAR(4) encode bytedict,
    Odometer VARCHAR(6) encode zstd,
    Transaction_Date VARCHAR(8) encode zstd,
    TransYYYYMM VARCHAR(6) encode bytedict,
    Drive_Type VARCHAR(3) encode zstd,
    Fuel_Type CHAR(1) encode zstd,
    Luxnon CHAR(1) encode zstd,
    Body_Type VARCHAR(15) encode zstd,
    Domimp CHAR(1) encode zstd,
    Veh_Type VARCHAR(5) encode zstd,
    Trans_Year VARCHAR(4) encode bytedict,
    Number_of_cars VARCHAR(3) encode zstd
)
DISTSTYLE KEY
DISTKEY (lems)
SORTKEY(LEMS);

INSERT INTO tblExternal42_191_201206_NEW
SELECT
    lems,
    vin,
    make,
    model,
    year,
    odometer,
    transaction_date,
    transyyyymm,
    drive_type,
    fuel_type,
    luxnon,
    bodytype,
    domimp,
    vehtype,
    transyear,
    car_number
FROM tblChild17_{build_id}_{build} WHERE LTRIM(RTRIM(lems)) <> '' AND lems IS NOT NULL;

UNLOAD ('SELECT o.* FROM (SELECT * FROM tblExternal42_191_201206_NEW LIMIT 100) AS o')
TO 's3://idms-7933-internalfiles/Reports/AuditReport_AutoLinkTable'
IAM_ROLE '{iam}'
KMS_KEY_ID '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
CLEANPATH
HEADER
PARALLEL OFF
;

DROP TABLE IF EXISTS tblExternal42_191_201206; 
ALTER TABLE tblExternal42_191_201206_NEW RENAME TO tblExternal42_191_201206;