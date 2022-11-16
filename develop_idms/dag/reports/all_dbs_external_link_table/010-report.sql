unload ('SELECT FR.DatabaseName,
	 FR.DBID,
       FR.GrossDatabaseCount,
       FR.NetDatabaseCount,
       FR.JoinOn,
       FR."DQI Individual",
       FR."DQI HH",
       FR.MGEN,
       FR."MGEN DELUXE",
       FR."Apogee Link",       
       FR.EXPERIAN,
       FR."PUBLISHER2",       
       FR."TargetReady",
       FR."Auto Link",
       FR."IG - US Business",
       FR."IG - Canadian Business",
       FR."D&B US Business Database",
       FR."Harte Hanks  US", 
       FR."Intent Database", 
       FR."Haystaq"       
 FROM FinalReport_ToBeDropped FR;')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
 