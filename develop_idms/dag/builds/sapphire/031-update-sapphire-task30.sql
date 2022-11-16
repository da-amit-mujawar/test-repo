/*
--Run only after you are done with SIC4_Prioritized  Fix.   and "Script 05.3  Part 1 Prioritized SIC to Industry Prioritized From txt.SQL"
--This Script used Part2 only.  if any changes, get a new  Excel file,  copy the formual and cut and past new SQL  commands
-- You can run entire script.

--ALWAYS  REPLACE {TASK(1|Result.Parameter.Value|MainTableName)}   with New buildid


--Cut and Past from Excel.  make sure the build table is correct.
*/

DROP TABLE IF EXISTS tblChild8_23091_202203;
CREATE TABLE tblChild8_23091_202203 (
    Individual_ID varchar(17), Industry varchar(10), filler1 varchar(2))
    DISTSTYLE key distkey ( individual_id )
    sortkey (individual_id);

copy tblChild8_23091_202203
from 's3://{s3-internal}/Sapphire/IQLoadFiles/Decode_71_{build_id}_Industry_IND.txt'
iam_role '{iam}'
ACCEPTINVCHARS
fixedwidth 'Individual_ID:17, Industry:10, filler1:2';


Update {sapphire-update1-ctas}
SET Industry_Prioritized = CASE
    WHEN B.Industry = 'I351' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN 'I351'
WHEN B.Industry = 'I106' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I106'
WHEN B.Industry = 'I224' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I224'
WHEN B.Industry = 'I311' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899')  THEN  'I311'
WHEN B.Industry = 'I102' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I102'
WHEN B.Industry = 'I215' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I215'
WHEN B.Industry = 'I373' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I373'
WHEN B.Industry = 'I365' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I365'
WHEN B.Industry = 'I371' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899')  THEN  'I371'
WHEN B.Industry = 'I370' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I370'
WHEN B.Industry = 'I125' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I125'
WHEN B.Industry = 'I368' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I368'
WHEN B.Industry = 'I313' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I313'
WHEN B.Industry = 'I302' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I302'
WHEN B.Industry = 'I312' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I312'
WHEN B.Industry = 'I374' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I374'
WHEN B.Industry = 'I211' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I211'
WHEN B.Industry = 'I306' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I306'
WHEN B.Industry = 'I114' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I114'
WHEN B.Industry = 'I309' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I309'
WHEN B.Industry = 'I369' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I369'
WHEN B.Industry = 'I310' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I310'
WHEN B.Industry = 'I121' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I121'
WHEN B.Industry = 'I222' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I222'
WHEN B.Industry = 'I218' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I218'
WHEN B.Industry = 'I208' AND (A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899') THEN  'I208'
WHEN B.Industry = 'I325' OR A.sic4_prioritized BETWEEN '0740' AND '0742'  THEN  'I325'
WHEN B.Industry = 'I332' THEN  'I332'
WHEN B.Industry = 'I322' OR A.sic4_prioritized BETWEEN '8011' AND '8049' THEN  'I322'
WHEN B.Industry = 'I386'  OR A.sic4_prioritized ='8082' THEN  'I386'
WHEN B.Industry = 'I326'  OR A.sic4_prioritized BETWEEN '8051' AND '8069' THEN  'I326'
WHEN B.Industry = 'I391' OR A.sic4_prioritized BETWEEN '8071' AND '8072'  THEN  'I391'
WHEN B.Industry = 'I398' THEN  'I398'
WHEN B.Industry = 'I363' OR A.sic4_prioritized ='8221' THEN  'I363'
WHEN B.Industry = 'I316' OR A.sic4_prioritized ='8211' THEN  'I316'
WHEN B.Industry = 'I362' OR A.sic4_prioritized ='8222' THEN  'I362'
WHEN B.Industry = 'I317' THEN  'I317'
WHEN B.Industry = 'I108' OR A.sic4_prioritized BETWEEN '8200' AND '8299' THEN  'I108'
WHEN B.Industry = 'I227' THEN  'I227'
WHEN B.Industry = 'I220' THEN  'I220'
WHEN B.Industry = 'I367' THEN  'I367'
WHEN B.Industry = 'I219' THEN  'I219'
WHEN B.Industry = 'I308' THEN  'I308'
WHEN B.Industry = 'I301' THEN  'I301'
WHEN B.Industry = 'I323' THEN  'I323'
WHEN B.Industry = 'I307' THEN  'I307'
WHEN B.Industry = 'I207' THEN  'I207'
WHEN B.Industry = 'I110' THEN  'I110'
WHEN B.Industry = 'I223' THEN  'I223'
WHEN B.Industry = 'I214' THEN  'I214'
WHEN B.Industry = 'I384' THEN  'I384'
WHEN B.Industry = 'I395' OR A.sic4_prioritized BETWEEN '8000' AND '8199' THEN  'I395'
WHEN B.Industry = 'I399' OR A.sic4_prioritized BETWEEN '8000' AND '8199' THEN  'I399'
WHEN B.Industry = 'I206' and A.sic4_prioritized NOT BETWEEN '8000' AND '8299'  AND A.sic4_prioritized NOT BETWEEN '9000' AND '9899' THEN  'I206'
WHEN B.Industry = 'I314' THEN  'I314'
WHEN B.Industry = 'I217' OR (A.sic4_prioritized ='9221' OR A.sic4_prioritized = '9224')  THEN  'I217'
WHEN B.Industry = 'I396' THEN  'I396'
WHEN B.Industry = 'I122' OR A.sic4_prioritized = '9711' THEN  'I122'
WHEN B.Industry = 'I111' THEN  'I111'
WHEN B.Industry = 'I205' THEN  'I205'
WHEN B.Industry = 'I401' OR A.sic4_prioritized = '9211' THEN  'I401'
WHEN B.Industry = 'I406' THEN  'I406'
WHEN B.Industry = 'I409' THEN  'I409'
WHEN B.Industry = 'I115' OR A.sic4_prioritized BETWEEN '9100' AND '9899'  THEN  'I115'
WHEN B.Industry = 'I388' THEN  'I388'
WHEN B.Industry = 'I387' THEN  'I387'
WHEN B.Industry = 'I303' THEN  'I303'
WHEN B.Industry = 'I305' THEN  'I305'
WHEN B.Industry = 'I304' THEN  'I304'
WHEN B.Industry = 'I381' THEN  'I381'
WHEN B.Industry = 'I107' THEN  'I107'
WHEN B.Industry = 'I389' THEN  'I389'
WHEN B.Industry = 'I201' THEN  'I201'
WHEN B.Industry = 'I104' OR A.sic4_prioritized = '8712'  THEN  'I104'
WHEN B.Industry = 'I318' THEN  'I318'
WHEN B.Industry = 'I353' THEN  'I353'
WHEN B.Industry = 'I109' OR A.sic4_prioritized = '8711' THEN  'I109'
WHEN B.Industry = 'I354' THEN  'I354'
WHEN B.Industry = 'I390' THEN  'I390'
WHEN B.Industry = 'I117' THEN  'I117'
WHEN B.Industry = 'I321' OR A.sic4_prioritized = '8721' THEN  'I321'
WHEN B.Industry = 'I330' THEN  'I330'
WHEN B.Industry = 'I319' THEN  'I319'
WHEN B.Industry = 'I320' THEN  'I320'
WHEN B.Industry = 'I119' THEN  'I119'
WHEN B.Industry = 'I112' THEN  'I112'
WHEN B.Industry = 'I113' THEN  'I113'
WHEN B.Industry = 'I101' THEN  'I101'
WHEN B.Industry = 'I315' THEN  'I315'
WHEN B.Industry = 'I397' OR A.sic4_prioritized BETWEEN '8000' AND '8199' THEN  'I397'
WHEN B.Industry = 'I328' THEN  'I328'
WHEN B.Industry = 'I340' THEN  'I340'
WHEN B.Industry = 'I105' THEN  'I105'
WHEN B.Industry = 'I212' THEN  'I212'
WHEN B.Industry = 'I355' THEN  'I355'
WHEN B.Industry = 'I324' THEN  'I324'
WHEN B.Industry = 'I120' OR A.sic4_prioritized BETWEEN '8100' AND '8199' THEN  'I120'
WHEN B.Industry = 'I383' THEN  'I383'
WHEN B.Industry = 'I352' THEN  'I352'
WHEN B.Industry = 'I382' THEN  'I382'
WHEN B.Industry = 'I385' THEN  'I385'
WHEN B.Industry = 'I204' THEN  'I204'
WHEN B.Industry = 'I203' THEN  'I203'
WHEN B.Industry = 'I209' THEN  'I209'
WHEN B.Industry = 'I213' THEN  'I213'
WHEN B.Industry = 'I202' THEN  'I202'
WHEN B.Industry = 'I103' OR A.sic4_prioritized BETWEEN '0100' AND '0999' THEN  'I103'
WHEN B.Industry = 'I226' THEN  'I226'
WHEN B.Industry = 'I392' OR A.sic4_prioritized = '8231' THEN  'I392'
WHEN B.Industry = 'I118' THEN  'I118'
WHEN B.Industry = 'I221' THEN  'I221'
WHEN B.Industry = 'I116' OR A.sic4_prioritized BETWEEN '8000' AND '8199' THEN  'I116'
WHEN B.Industry = 'I331' THEN  'I331'
WHEN B.Industry = 'I216' THEN  'I216'
WHEN B.Industry = 'I341' THEN  'I341'
WHEN B.Industry = 'I124' THEN  'I124'
WHEN B.Industry = 'I327' THEN  'I327'
WHEN B.Industry = 'I329' THEN  'I329'
WHEN B.Industry = 'I402' THEN  'I402'
WHEN B.Industry = 'I404' THEN  'I404'
WHEN B.Industry = 'I405' THEN  'I405'
WHEN B.Industry = 'I407' THEN  'I407'
WHEN B.Industry = 'I408' THEN  'I408'
WHEN B.Industry = 'I999' THEN  'I999'
    WHEN A.Industry_Prioritized = '' THEN 'I999' END
FROM {sapphire-update1-ctas} A
    INNER Join tblChild8_23091_202203 B
on A.Individual_ID =B.Individual_ID
       And  A.Industry_Prioritized ='';


CREATE TABLE #CompanyIndustryPrioritized (COMPANY_MC VARCHAR(15),Industry_Prioritized VARCHAR (6));

copy #CompanyIndustryPrioritized
from 's3://{s3-internal}{s3-key-buildsupportfiles}/Company MC Pipe Industry Priority.txt'
iam_role '{iam}'
ACCEPTINVCHARS FILLRECORD
delimiter '|'
ignoreheader as 1;


Delete from #CompanyIndustryPrioritized where COMPANY_MC ='' or Industry_Prioritized ='';

Update #CompanyIndustryPrioritized  SET Industry_Prioritized = UPPER(LTRIM(RTRIM(Industry_Prioritized)));

/*
Select count(*), count(Distinct COMPANY_MC) from #CompanyIndustryPrioritized;
count(),count(distinct #CompanyIndustryPrioritized.COMPANY_MC)
514,514
*/

UPDATE {sapphire-update1-ctas}
   SET Industry_Prioritized = B.Industry_Prioritized
  FROM {sapphire-update1-ctas} tblMain
 INNER JOIN #CompanyIndustryPrioritized  B ON UPPER(tblMain.Company_MC) = UPPER(B.Company_MC);


DROP TABLE IF EXISTS #CompanyIndustryPrioritized;
