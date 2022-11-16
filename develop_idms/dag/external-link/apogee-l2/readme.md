# External Links - L2 Voter Data 
Uploads External45 from S3

## Key Data Elements


## Dependencies and Data Axle Sources
VC job for External Links - LOAD APOGEE L2 (EXTERNAL 45) - creates uploaded file

## Processing


## Testing



## UPCOMING mods
- "email_address" : "Maria.Bartell@data-axle.com, Eric.Freeberg@data-axle.com, IDMSAdminConsumerPearl@infogroup.com, IDMSAdminB2BPearl@infogroup.com",
- set up sftp.L2Political.com job to download voter frequency file (ck w/Ram/databricks, if no contact mft)
  - see VC connection sftp.L2Political.com for login details (sftp ssh - secure shell)
  - file mask: /*VoteFrequency.csv
  - currently downloaded to: \\stcsanisln01-idms\idms\neptune\IDMSFILES\Temp\
  - then renamed to VoteFrequency.csv
- Consider VC Job: ETL AWS EXPORT - Apogee L2 Comp&Indiv ID Reference Table.
  - This was created for development testing of APOGEE B1 LOAD TRANSACTIONS
  - 
  - l2 list conversion config items removed
    "s3-key1": "/neptune_apogee/L2/Apogee_L2_Export_All.txt",
    "s3-key2": "axle-raw-customer-sources/apogee/L2",
    "reportname1": "/Reports/AuditReport_L2Voter_Sample_External45",
    "reportname2": "/Reports/AuditReport_L2Voter_CountByListSource_External45",
    "reportname3": "/Reports/AuditReport_L2Voter_VoterActivity_External45",
    "reportname4": "/Reports/1182_ApogeeL2_DQI",
   

## Modifications ##



## Created By
* ** Susan Fu ** - *Initial*
* Caroline Burch - *added new fields, email message*
* Caroline Burch - removed list load processing, load External45 from S3 only

## NOTES email_message
