CREATE OR REPLACE PROCEDURE sp_MDR_JobRollup ()
LANGUAGE plpgsql
AS $$
DECLARE
  loopStart INTEGER;
  loopEnd INTEGER;
  strSQL VARCHAR(max) ;
  intRowcount INTEGER;
BEGIN
  --this procedure assume the MDR_JobRollupFinal_TOBEDELETED & MDR_WORK_TOBEDELETED populated in previous task

  DROP TABLE IF EXISTS mdr_jobrollup_temp_tobedropped;
  CREATE TABLE mdr_jobrollup_temp_tobedropped
  (
  individual_id varchar(17),
  pubcode varchar(10),
  pkid int
  );


  loopStart = 1;
  SELECT MAX(Counts) INTO loopEnd from MDR_JobRollupFinal_TOBEDELETED;
  WHILE (loopStart <=loopEnd)
  LOOP

    INSERT INTO mdr_jobrollup_temp_tobedropped (individual_id,pubcode, pkid)
    SELECT a.individual_id,a.pubcode,min(a.pkid) pkid
    	from MDR_WORK_TOBEDELETED A
    	JOIN MDR_JobRollupFinal_TOBEDELETED B ON A.individual_id=B.individual_id and A.pubcode=B.pubcode
    	WHERE a.processed IS NULL
    	Group by a.individual_id,a.pubcode;

    If loopStart=1 THEN
    		update MDR_JobRollupFinal_TOBEDELETED
    			SET JobRollup =','+ B.dbvalue +','
    			FROM MDR_JobRollupFinal_TOBEDELETED A
    			JOIN MDR_WORK_TOBEDELETED B ON A.individual_id=B.individual_id and A.pubcode=B.pubcode
    			JOIN mdr_jobrollup_temp_tobedropped C ON B.Individual_Id=C.Individual_Id and B.PubCode=C.PubCode AND B.pkid=C.pkid;
    	ELSE
    		update MDR_JobRollupFinal_TOBEDELETED
    			SET JobRollup = A.JobRollup + B.dbvalue +','
    			FROM MDR_JobRollupFinal_TOBEDELETED A
    			JOIN MDR_WORK_TOBEDELETED B ON A.individual_id=B.individual_id and A.pubcode=B.pubcode
    			JOIN mdr_jobrollup_temp_tobedropped C ON B.Individual_Id=C.Individual_Id and B.PubCode=C.PubCode AND B.pkid=C.pkid;
      END IF;

  	UPDATE MDR_WORK_TOBEDELETED
    	SET processed='Y'
    	FROM MDR_WORK_TOBEDELETED A
    	JOIN mdr_jobrollup_temp_tobedropped B ON A.Individual_Id=B.Individual_Id and A.PubCode=B.PubCode AND A.pkid=B.pkid;

    Truncate table mdr_jobrollup_temp_tobedropped  ;
    loopStart = loopStart + 1;

  END LOOP;
  DROP TABLE IF EXISTS mdr_jobrollup_temp_tobedropped;
END;
$$;
/


