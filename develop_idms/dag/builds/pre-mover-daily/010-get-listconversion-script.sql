/*Set to onestep if not done.*/
UPDATE TOP(1) A
SET iIsOneStep = 1 
FROM dw_admin.dbo.tblbuild A
WHERE ID = {build_id}  AND iIsOneStep = 0

UPDATE TOP (1) tblBuildLoL
SET cSourceFileNameReadyToLoad = 's3://{s3-internal}/premovers/premover_{yyyy}{mm}{dd}.txt',
      cSystemFileNameReadyToLoad = 's3://{s3-internal}/premovers/premover_{yyyy}{mm}{dd}.txt'
 WHERE MasterLoLID = (SELECT TOP 1 ID FROM tblMasterLoL WHERE DatabaseID = {dbid} AND iIsActive = 1)
   AND BuildID = {build_id}

UPDATE tblListLoadStatus
   SET iIsCurrent = 0
  FROM tblListLoadStatus
 WHERE BuildLoLID IN (SELECT TOP 1 ID FROM tblBuildLoL WHERE BuildID = {build_id} AND iIsActive = 1)
   AND iIsCurrent = 1

DELETE TOP (2)
  FROM tblLoadProcessStatus
 WHERE BuildLoLID IN (SELECT TOP 1 ID FROM tblBuildLoL WHERE BuildID = {build_id} AND iIsActive = 1)
   AND iStatus in (5,1000)

UPDATE TOP (1) tblListLoadStatus
   SET iIsCurrent = 1
  FROM tblListLoadStatus
 WHERE BuildLoLID IN (SELECT TOP 1 ID FROM tblBuildLoL WHERE BuildID = {build_id} AND iIsActive = 1)
   AND iIsCurrent = 0
   AND LK_LoadStatus = '70'
