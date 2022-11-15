
DROP TABLE IF EXISTS spectrumdb.myinfo_campaigns;

CREATE EXTERNAL TABLE spectrumdb.myinfo_campaigns (
	   Id numeric(11,0),
   Description varchar(200),
   CampaignTypeId numeric(3, 0),
   CampaignSubCategoryId numeric(3,0),
   PhoneNumber varchar(16),
   DNIS varchar(6),
   DisplayedSalesforce varchar(30),
   DivisionId varchar(6),
   DefaultVendorId varchar(6),
   VendorCode varchar(6),
   LandingPageURL char(500),
   UniqueURL char(255),
   SaleRepId varchar(6),
   PromoCode varchar(10),
   ProductCode varchar(10),
   ActivationFee DOUBLE PRECISION,
   MonthlyPrice DOUBLE PRECISION,
   YearlyPrice DOUBLE PRECISION,
   Cost numeric(18,0),
   HDYHAUAvailable char(1),
   IsDeleted char(1),
   CreatedBy varchar(50),
   CreatedDateTime timestamp,
   LastModifiedBy varchar(50),
   LastModifiedDateTime timestamp,
   AvailableInMyInfo char(1),
   LastMyInfoSynch timestamp,
   AvailableInSFNA5 char(1),
   LastSFNA5Synch timestamp,
   SFNA5Id varchar(18),
   AvailableInSFNA4 char(1),
   LastSFNA4Synch timestamp,
   SFNA4Id varchar(18),
   AvailableInISIS char(1),
   EmailTemplateId numeric(3,0),
   EmailTemplate2Id numeric(3,0),
   EmailFromId numeric(3,0),
   EmailCCId numeric(3,0),
   EmailBCCId numeric(3,0),
   SFUserIdEE varchar(18),
   SFUserIdSG varchar(18),
   PhoneDescriptionField varchar(16),
   CampaignSubCategoryDetailId numeric(3,0),
   CampaignSubCategoryDetailText varchar(255),
   CampaignPhoneTypeId numeric(3,0),
   Notes varchar(255),
   SFRecordTypeIdEE varchar(18),
   SFRecordTypeIdSG varchar(18),
   MediaCode varchar(40),
   IsHDYHAU char(1),
   StartDateTime timestamp,
   EndDateTime timestamp,
   Display varchar(1),
   SortOrder int,
   MediaCodeTypeId numeric(5,0),
   OESSSourceCode varchar(50),
   ParentCampaignId varchar(18),
   Keyword varchar(80),
   BatchNumber int,
   ScreenPop varchar(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = '|',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS textfile
LOCATION 's3://axle-internal-sources/raw/myinfo/myinfo_campaigns/'
--TABLE PROPERTIES ('skip.header.line.count'='1');
TABLE PROPERTIES ('compression_type'='gzip');

 
-------------------------------------------
-- Create a view in interna schema
-------------------------------------------
DROP VIEW IF EXISTS interna.myinfo_campaigns;

CREATE VIEW interna.myinfo_campaigns
AS 
SELECT * 
  FROM spectrumdb.myinfo_campaigns
  WITH NO SCHEMA BINDING;
 

-- Verify Counts
SELECT COUNT(*) FROM interna.myinfo_campaigns
SELECT * FROM interna.myinfo_campaigns

select count(*)from spectrumdb.myinfo_campaigns

select top 100 * from spectrumdb.myinfo_campaigns

