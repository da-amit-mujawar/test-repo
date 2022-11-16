DROP TABLE IF EXISTS nosuchtable;
DROP TABLE IF EXISTS Exclude_ConsEmail4Apogee;

CREATE TABLE Exclude_ConsEmail4Apogee (
EmailAddress VARCHAR(80) ENCODE ZSTD,
  Digitalmatch VARCHAR(1) ENCODE ZSTD,
  Opt_Out_Flag VARCHAR(1) ENCODE ZSTD,
  Vendor_Code VARCHAR(2) ENCODE ZSTD,
  DOMAIN VARCHAR(80) ENCODE ZSTD,
  Top_Level_Domain VARCHAR(80) ENCODE ZSTD,
  md5_email_lower VARCHAR(32) ENCODE ZSTD,
  md5_email_upper VARCHAR(32) ENCODE ZSTD,
  sha256_email VARCHAR(64) ENCODE ZSTD,
  sha512_email VARCHAR(128) ENCODE ZSTD,
  BV_Flag VARCHAR(1) ENCODE ZSTD,
  Marigold VARCHAR(1) ENCODE ZSTD,
  BVT_Refresh_Date VARCHAR(8) ENCODE BYTEDICT,
  IPST_Refresh_Date VARCHAR(8) ENCODE BYTEDICT,
  BestDate VARCHAR(8) ENCODE ZSTD,
  Mgen_Match_Flag VARCHAR(1) ENCODE ZSTD,
  Bridge_Code VARCHAR(1) ENCODE ZSTD,
  Best_Date_Range VARCHAR(1) ENCODE ZSTD,
  cInclude VARCHAR(1) ENCODE ZSTD,
  Email_Deliverable VARCHAR(1) ENCODE ZSTD,
  Email_Marketable VARCHAR(1) ENCODE ZSTD,
  Email_Reputation_Risk VARCHAR(1) ENCODE ZSTD,
  Email_Deployable VARCHAR(1) ENCODE ZSTD,
  Individual_ID VARCHAR(25) ENCODE ZSTD,
  Company_ID VARCHAR(25) ENCODE ZSTD
);

INSERT INTO Exclude_ConsEmail4Apogee
SELECT DISTINCT
trim(EmailAddress) as emailaddress,
Digitalmatch,
Opt_Out_Flag,
Vendor_Code,
trim(Domain) as domain,
trim(Top_Level_Domain) as Top_Level_Domain,
md5_email_lower,
md5_email_upper,
sha256_email,
sha512_email,
BV_Flag,
Marigold,
BVT_Refresh_Date,
IPST_Refresh_Date,
BestDate,
Mgen_Match_Flag,
Bridge_Code,
Best_Date_Range,
cInclude,
Email_Deliverable,
Email_Marketable,
Email_Reputation_Risk,
Email_Deployable,
Individual_ID,
Company_Id
FROM {maintable_name}
WHERE nvl(EmailAddress,'') <> ''
;

unload ('select * from  Exclude_ConsEmail4Apogee')
to 's3://idms-7933-internalfiles/neptune_apogee/ConsEmail4Apogee.txt'
iam_role '{iam}'
encrypted
parallel off
allowoverwrite
;
