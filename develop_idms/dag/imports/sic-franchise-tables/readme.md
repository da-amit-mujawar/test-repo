# idms-dag-imports-tblsiccode

##NOTE: dw_admin.dbo.tblsiccode 
sql table has two additional cols: ID, cIndicator
cIndicator is updated for matching codes in SICFranchise and IndustryCode tables. 
This is only needed for sql server, per JP

**non-db specific** <br />
*imported from datasync S3 folder* <br />
*file is refreshed by mmdb in network monthly delivery folder* <br /> 
*runs @3am on 3rd day of every month*

