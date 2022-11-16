DROP TABLE IF EXISTS nosuchtable;
DROP TABLE IF EXISTS #fastcount_temp;
    
WITH top3rownbrs AS
(
    SELECT  cons_individualid, 
            bus_cottagecode, 
            bus_contactliteraltitlecode, 
            bus_professionaltitle, 
            bus_FunctionalAreaCode, 
            bus_RoleCode,
            TRIM(bus_cottagecode)||','||TRIM(bus_contactliteraltitlecode)||','||TRIM(bus_professionaltitle)||','||TRIM(Bus_FunctionalAreaCode)||','||TRIM(bus_RoleCode) AS jobtitles,
            ROW_NUMBER() OVER (PARTITION BY cons_individualid ) rownbr
    FROM {maintable_name} tblcons 
    LEFT JOIN tblMain_{build_id_b2c}_{build_b2c} tblb2c 
    ON tblcons.individual_id = tblb2c.cons_individualid
    GROUP BY cons_individualid,
            bus_cottagecode,
            bus_contactliteraltitlecode, 
            bus_professionaltitle, 
            bus_FunctionalAreaCode,
            bus_RoleCode
    ORDER BY cons_individualid, rownbr
  )
  SELECT * INTO #fastcount_temp
  FROM top3rownbrs
  WHERE rownbr <= 3;

-- flatten with pivot
DROP TABLE IF EXISTS #ExactData_B2C;
SELECT * INTO #ExactData_B2C
FROM (
        SELECT cons_individualid, 
                jobtitles, 
                rownbr 
        FROM #fastcount_temp
        )
        PIVOT (MAX(jobtitles) FOR rownbr IN (1 AS JOB_TITLE_1,2 AS JOB_TITLE_2,3 AS JOB_TITLE_3));



--Create tblchild15
DROP TABLE IF EXISTS tblChild15_{build_id}_{build};
CREATE TABLE tblChild15_{build_id}_{build}
(
    individual_id VARCHAR(21)   ENCODE zstd
	,job_title_1 VARCHAR(24)   ENCODE zstd
	,job_title_2 VARCHAR(24)   ENCODE zstd
	,job_title_3 VARCHAR(24)   ENCODE zstd
)
DISTSTYLE AUTO
DISTKEY (individual_id);

INSERT INTO tblChild15_{build_id}_{build}
(
    individual_id,
    job_title_1,
    job_title_2,
    job_title_3
)
SELECT * FROM #ExactData_B2C;


--tblchild16 using consumer maintable and b2c maintable
DROP VIEW IF EXISTS tblChild16_{build_id}_{build};
CREATE VIEW tblChild16_{build_id}_{build}
AS 
SELECT 
    tblcons.Individual_ID as Individual_ID,
    tblb2c.bus_cottagecode as bus_cottagecode,
    tblb2c.bus_contactliteraltitlecode as bus_contactliteraltitlecode , 
    tblb2c.bus_professionaltitle as bus_professionaltitle , 
    tblb2c.bus_FunctionalAreaCode as bus_FunctionalAreaCode , 
    tblb2c.bus_RoleCode as bus_RoleCode,
    tblb2c.bus_ContactLiteralTitleCode_Description as bus_ContactLiteralTitleCode_Description
FROM {maintable_name} tblcons
LEFT JOIN tblmain_{build_id_b2c}_{build_b2c} tblb2c
ON tblcons.individual_id = tblb2c.cons_individualid
GROUP BY Individual_ID,
bus_cottagecode,
bus_contactliteraltitlecode,
bus_professionaltitle,
bus_FunctionalAreaCode,
bus_RoleCode,
bus_ContactLiteralTitleCode_Description;