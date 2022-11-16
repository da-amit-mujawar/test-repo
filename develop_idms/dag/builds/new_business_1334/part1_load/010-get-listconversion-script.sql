/*
Reju Mathew 2021.04.20.
Jira 847

NUA RedShift ETL | VC1 LIST_UPDATES -- INFOGROUP NEW BUSINESS (1334) -PART 1 - LOAD - WEEKLY (WEDS)


*/
/*
if Odd Week number,  use Build 19273
if Even week number, use Build 19272

divide  week no by 2.  if reminder is 1, its a odd week

Reju Mathew 2020.03.24.   #ticket 843729
*/


DECLARE @Buildid int, @Buildid_Even int, @Buildid_Odd int,  @Listid int

SET @Buildid_Even= {evenbuildid}
SET @Buildid_Odd= {oddbuildid}
Set @Listid  = {listid}

--set buildid
--if (DATENAME ( ww , getdate() )  % 2 ) = 0
--	SET @Buildid =  @Buildid_Even  /*Even weeks build id*/
--ELSE  SET @Buildid =@Buildid_Odd   /*Odd weeks build id*/

--20211220 CB: fix logic to select correct buildid
SET @Buildid = CASE WHEN (DATEPART(w, getdate()) % 2) = 0 THEN @Buildid_Even ELSE @Buildid_Odd END;

/* deactivate automation CB 2022.01.27
-- start build process
UPDATE TOP (1) dw_admin.dbo.tblBuild
   SET LK_BuildStatus='10',
      iIsOndisk =0,
      cDescription= 'Weekly Build: '+  FORMAT (getdate()+1, 'MMM-dd-yyyy') + ' - activation pending' ,
      dModifiedDate = GETDATE(),
      dScheduledDateTime = GETDATE(),
      cModifiedBy   = '{updateuserid}'
WHERE ID = @Buildid

/*
Not needed as these values are set from ETL-UI

UPDATE TOP (1) dw_admin.dbo.tblBuildLoL
   SET cSourceFileNameReadyToLoad = 's3://{s3-internal}{s3-key1}',
       cSystemFileNameReadyToLoad = 's3://{s3-internal}{s3-key1}'
 WHERE MasterLoLID = @Listid
   AND BuildID = @Buildid
*/

--initialize listload status
UPDATE tblListLoadStatus
   SET iIsCurrent = 0
  FROM dw_admin.dbo.tblListLoadStatus tblListLoadStatus
 WHERE BuildLoLID IN (SELECt TOP 1 ID FROM dw_admin.dbo.tblBuildLoL WHERE MasterLoLID = @Listid AND BuildID = @Buildid)
   AND iIsCurrent = 1

DELETE TOP (2)
  FROM dw_admin.dbo.tblLoadProcessStatus
 WHERE BuildLoLID IN (SELECt TOP 1 ID FROM dw_admin.dbo.tblBuildLoL WHERE MasterLoLID = @Listid AND BuildID = @Buildid)
   AND iStatus in (5,1000)

--set listload status to waiting for approval
UPDATE TOP (1) tblListLoadStatus
   SET iIsCurrent = 1
  FROM dw_admin.dbo.tblListLoadStatus tblListLoadStatus
 WHERE BuildLoLID IN (SELECt TOP 1 ID FROM dw_admin.dbo.tblBuildLoL WHERE MasterLoLID = @Listid AND BuildID = @Buildid)
   AND iIsCurrent = 0
   AND LK_LoadStatus = '70'
*/