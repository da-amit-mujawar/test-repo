DROP TABLE IF EXISTS reports.media_code_lookup_data;

CREATE TABLE reports.media_code_lookup_data
(Division NVARCHAR(100) NULL,
Division_Code NVARCHAR(100) NULL,
Channel NVARCHAR(100) NULL,
Channel_Code NVARCHAR(100) NULL,
Source NVARCHAR(100) NULL,
Sub_Source NVARCHAR(100) NULL,
Sub_Source_Code NVARCHAR(100) NULL, 
Sub_Source_Code_Key NVARCHAR(100) NULL,
Media_Code_Prefix NVARCHAR(100) NULL);

COPY reports.media_code_lookup_data
FROM 's3://{s3-bi-reports}/Updated_media_code_merged_file_to_s3.csv'
IAM_ROLE '{iam}'
FORMAT csv
DELIMITER ',';

DROP TABLE IF EXISTS reports.media_code_legacy_data;

CREATE TABLE reports.media_code_legacy_data
(Old_Media_Code_Prefix NVARCHAR(100) NULL,
Media_Code_Prefix NVARCHAR(100) NULL);

COPY reports.media_code_legacy_data
FROM 's3://{s3-bi-reports}/Media_Codes_GAAP_Reclassification_20210308.csv'
IAM_ROLE '{iam}'
FORMAT CSV
DELIMITER ',';

--------------------------------------
--------------------------------------
DROP TABLE IF EXISTS reports.media_code_lookup_legacy;

SELECT  DISTINCT  
division division_tag,
channel,
CONCAT(concat(division,'-'),channel) division_channel,
CONCAT(concat(channel,'-'),sub_source_code_key) channel_sub_souce_key,
old_media_code_prefix,l.media_code_prefix 
into reports.media_code_lookup_legacy
 from reports.media_code_lookup_data m
join reports.media_code_legacy_data l on
m.media_code_prefix=l.media_code_prefix;

DROP TABLE IF EXISTS reports.media_code_lookup;

SELECT DISTINCT division division_tag,
channel,
CONCAT(concat(division,'-'),channel) division_channel,
CONCAT(concat(channel,'-'),sub_source_code_key) channel_sub_souce_key,
media_code_prefix 
INTO reports.media_code_lookup
FROM reports.media_code_lookup_data m;
