-- Remove Duplicates in TblExternal45_191_201206_NEW by Individual_ID

DROP TABLE IF EXISTS L2_LALVoterID_ToBeDropped; 
CREATE TABLE L2_LALVoterID_ToBeDropped (L2_LALVoterID VARCHAR(14)); 

INSERT INTO L2_LALVoterID_ToBeDropped (L2_LALVoterid)
SELECT L2_LALVoterID
FROM {tablename1}
WHERE L2_LALVoterID NOT IN (
	SELECT  min(L2_LALVoterID) 
	FROM {tablename1} 
	--WHERE individual_id <>0 
	GROUP BY individual_id);  
	
DELETE FROM {tablename1} WHERE L2_LALVoterid IN
(SELECT L2_LALVoterid FROM L2_LALVoterID_ToBeDropped); 

-- Approve List 17949 for all existing Mailers in DWAP
