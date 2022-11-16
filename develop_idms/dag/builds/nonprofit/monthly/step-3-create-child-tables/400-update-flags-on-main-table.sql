DROP TABLE IF EXISTS No_Such_Table;

-- No longer needed
/*
DROP TABLE IF EXISTS tblChild17_{build_id}_{build};

CREATE TABLE tblChild17_{build_id}_{build}
  DISTKEY (company_id)
  SORTKEY (company_id)
AS
SELECT m.company_id,
       CASE WHEN MAX(child2.company_id) IS NULL THEN 'N' ELSE 'Y' END AS MgenMatch,
       CASE WHEN MAX(child1.company_id) IS NULL THEN 'N' ELSE 'Y' END AS DonorMatch,
       CASE WHEN MAX(child5.company_id) IS NULL THEN 'N' ELSE 'Y' END AS FECMatch,
       CASE WHEN MAX(ext45.company_id) IS NULL THEN 'N' ELSE 'Y' END AS L2VoterMatch
  FROM tblmain_{build_id}_{build} m
  LEFT JOIN (SELECT company_id FROM tblchild2_{build_id}_{build} GROUP BY company_id) child2
    ON m.company_id = child2.company_id
  LEFT JOIN (SELECT company_id FROM tblchild1_{build_id}_{build} GROUP BY company_id) child1
    ON m.company_id = child1.company_id
  LEFT JOIN (SELECT company_id FROM tblchild5_{build_id}_{build} GROUP BY company_id) child5
    ON m.company_id = child5.company_id
  LEFT JOIN (SELECT company_id FROM tblExternal45_191_201206 GROUP BY company_id) ext45
    ON m.company_id = ext45.company_id
 GROUP BY m.company_id;


DROP TABLE IF EXISTS tblChild18_{build_id}_{build};

CREATE TABLE tblChild18_{build_id}_{build}
  DISTKEY (individual_id)
  SORTKEY (INDIVIDUAL_ID)
AS
SELECT m.individual_id,
       CASE WHEN MAX(child3.individual_id) IS NULL THEN 'N' ELSE 'Y' END AS IndividualDonorMatch,
       CASE WHEN MAX(child4.individual_id) IS NULL THEN 'N' ELSE 'Y' END AS FECIndividualMatch,
       CASE WHEN MAX(ext45_i.individual_id) IS NULL THEN 'N' ELSE 'Y' END AS L2VoterIndividualMatch,
       CASE WHEN MAX(ext44.individual_id) IS NULL THEN 'N' ELSE 'Y' END AS HaystaqIndividualMatch
  FROM tblmain_{build_id}_{build} m
  LEFT JOIN (SELECT individual_id FROM tblchild3_{build_id}_{build} GROUP BY individual_id) child3
    ON m.individual_id = child3.individual_id
  LEFT JOIN (SELECT individual_id FROM tblchild4_{build_id}_{build} GROUP BY individual_id) child4
    ON m.individual_id = child4.individual_id
  LEFT JOIN (SELECT individual_id FROM tblExternal45_191_201206 GROUP BY individual_id) ext45_i
    ON m.individual_id = ext45_i.individual_id
  LEFT JOIN (SELECT individual_id FROM tblExternal44_191_201206 GROUP BY individual_id) ext44
    ON m.individual_id = ext44.individual_id
 GROUP BY m.individual_id;
*/


/*
No usage as of 8/11/2022 as per Jayesh

--Add multibuyercount (Recency) Flag
SELECT DISTINCT SourceListID
INTO #sourcelistid
FROM tblChild3_{TASK(1|StdOut)}_new ; commit;

SELECT COUNT(DISTINCT sourcelistid) nCount, individual_id
  INTO #tmpnp_recency
  FROM tblChild3_{TASK(1|StdOut)}_new
 WHERE sourcelistid IN (SELECT  SourceListID  FROM #sourcelistid)  
 GROUP BY individual_id; COMMIT;

CREATE HG INDEX i1 ON #tmpnp_recency (individual_id);

UPDATE tblDQI_new   
   SET multibuyercount = B.nCount
  FROM tblDQI_new    
 INNER JOIN #tmpnp_recency B
    ON tblDQI_new.individual_id = B.individual_id; COMMIT;

DROP TABLE #tmpnp_recency;COMMIT;
DROP TABLE #sourcelistid;COMMIT;
*/

/*
Not sure if this is needed

UPDATE tblDQI_new
SET LTD_Number_Of_List_Source_DQI = b.LTD_Number_Of_List_Sources
FROM tblDQI_new
INNER JOIN tblChild1_{TASK(1|StdOut)}_new   b		--HH Summaries
ON tblDQI_new.company_id = b.company_id;
commit;
*/
