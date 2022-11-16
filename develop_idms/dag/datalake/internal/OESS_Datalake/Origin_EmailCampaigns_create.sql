DROP TABLE IF EXISTS spectrumdb.Origin_EmailCampaigns; 
CREATE EXTERNAL TABLE spectrumdb.Origin_EmailCampaigns 
(
Id INT
,UserId INT
,CampaignName VARCHAR(150)
,BlastSessionId Varchar(150)
,EmailBlobFilePath VARCHAR(250)
,State SMALLINT
,"Type" SMALLINT
,EmailCount INT
,Price DECIMAL(18,2)
,ModuleId SMALLINT
,RequestKey varchar(150)
,ParentId INT
,IsDeleted int
,DeletedDate TIMESTAMP
,CreateDate TIMESTAMP
,CampaignOrderId INT
,CampaignType SMALLINT
,SavedSearchUniqueName varchar(150)
,DatalynxSavedSearch varchar(150)
,RecordSetUniqueName varchar(150)
,TeamListId INT
,FinalCount INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
'separatorChar' = '|', 
'quoteChar' = '"', 
'escapeChar' = '\\' 
) 
STORED AS textfile 
LOCATION 's3://axle-internal-sources/raw/oess/Origin_EmailCampaigns/' 
TABLE PROPERTIES ('compression_type'='gzip'); 
DROP VIEW IF EXISTS interna.Origin_EmailCampaigns; 
CREATE VIEW interna.Origin_EmailCampaigns AS SELECT * FROM spectrumdb.Origin_EmailCampaigns WITH NO SCHEMA BINDING; 
SELECT COUNT(*) FROM interna.Origin_EmailCampaigns; 
SELECT TOP 100 * FROM interna.Origin_EmailCampaigns; 
