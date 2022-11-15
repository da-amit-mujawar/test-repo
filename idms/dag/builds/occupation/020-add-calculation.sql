DROP TABLE IF EXISTS temp_{tablename1};
CREATE TABLE temp_{tablename1}
(
	lastname VARCHAR(20) ENCODE zstd,
	firstname VARCHAR(15) ENCODE zstd,
	middlename VARCHAR(15) ENCODE zstd,
	title VARCHAR(1) ENCODE zstd,
	gender VARCHAR(1) ENCODE zstd,
	addressline1 VARCHAR(40) ENCODE zstd,
	city VARCHAR(28) ENCODE zstd,
	state VARCHAR(2) ENCODE zstd,
	zip VARCHAR(5) ENCODE zstd,
	zipfour VARCHAR(4) ENCODE zstd,
	carrierroutecode VARCHAR(4) ENCODE bytedict,
	delivery_point_bar_code VARCHAR(3) ENCODE zstd,
	industrytitle VARCHAR(50) ENCODE zstd,
	occupationtitle VARCHAR(50) ENCODE zstd,
	specialtytitle VARCHAR(50) ENCODE zstd,
	censusstatecode VARCHAR(2) ENCODE zstd,
	censuscountycode VARCHAR(3) ENCODE zstd,
	censustract VARCHAR(6) ENCODE zstd,
	censusblockgroup VARCHAR(1) ENCODE zstd,
	highrise VARCHAR(2) ENCODE zstd,
	yearlicensed VARCHAR(4) ENCODE bytedict,
	licensestate VARCHAR(2) ENCODE zstd,
	licensenumber VARCHAR(25) ENCODE zstd,
	enhancedphone VARCHAR(1) ENCODE zstd,
	area_code VARCHAR(3) ENCODE zstd,
	phone_number VARCHAR(7) ENCODE zstd,
	time_zone VARCHAR(1) ENCODE zstd,
	numberofchildren VARCHAR(1) ENCODE zstd,
	childrenbygender0 VARCHAR(1) ENCODE zstd,
	childrenbygender1 VARCHAR(1) ENCODE zstd,
	childrenbygender2 VARCHAR(1) ENCODE zstd,
	childrenbygender3 VARCHAR(1) ENCODE zstd,
	childrenbygender4 VARCHAR(1) ENCODE zstd,
	childrenbygender5 VARCHAR(1) ENCODE zstd,
	childrenbygender6 VARCHAR(1) ENCODE zstd,
	childrenbygender7 VARCHAR(1) ENCODE zstd,
	childrenbygender8 VARCHAR(1) ENCODE zstd,
	childrenbygender9 VARCHAR(1) ENCODE zstd,
	childrenbygender10 VARCHAR(1) ENCODE zstd,
	childrenbygender11 VARCHAR(1) ENCODE zstd,
	childrenbygender12 VARCHAR(1) ENCODE zstd,
	childrenbygender13 VARCHAR(1) ENCODE zstd,
	childrenbygender14 VARCHAR(1) ENCODE zstd,
	childrenbygender15 VARCHAR(1) ENCODE zstd,
	childrenbygender16 VARCHAR(1) ENCODE zstd,
	childrenbygender17 VARCHAR(1) ENCODE zstd,
	length_of_residence VARCHAR(2) ENCODE zstd,
	own_rent_indicator VARCHAR(1) ENCODE zstd,
	find_income VARCHAR(6) ENCODE zstd,
	activebankcard VARCHAR(1) ENCODE zstd,
	donor_ever VARCHAR(1) ENCODE zstd,
	home_value_code VARCHAR(1) ENCODE zstd,
	cat_owner VARCHAR(1) ENCODE zstd,
	dog_owner VARCHAR(1) ENCODE zstd,
	mail_responsive VARCHAR(1) ENCODE zstd,
	mail_responsive_previous VARCHAR(1) ENCODE zstd,
	mail_order VARCHAR(1) ENCODE zstd,
	marital_status VARCHAR(1) ENCODE zstd,
	health_contributor VARCHAR(1) ENCODE zstd,
	political_contributor VARCHAR(1) ENCODE zstd,
	religious_contirbutor VARCHAR(1) ENCODE zstd,
	personal_computer VARCHAR(1) ENCODE zstd,
	age VARCHAR(3) ENCODE bytedict,
	latitude VARCHAR(9) ENCODE zstd,
	longitude VARCHAR(9) ENCODE zstd,
	geo_match_code VARCHAR(1) ENCODE zstd,
	cbsa_code VARCHAR(5) ENCODE zstd,
	cbsa_code_description VARCHAR(50) ENCODE zstd,
	cbsa_level_description VARCHAR(5) ENCODE zstd,
	county_code VARCHAR(3) ENCODE zstd,
	mailscore VARCHAR(2) ENCODE zstd,
	housenumber VARCHAR(10) ENCODE zstd,
	streetpredirectional VARCHAR(2) ENCODE zstd,
	streetname VARCHAR(28) ENCODE zstd,
	streetsuffix VARCHAR(4) ENCODE zstd,
	streetpostdirectional VARCHAR(2) ENCODE zstd,
	unittype VARCHAR(4) ENCODE zstd,
	unitnumber VARCHAR(8) ENCODE zstd,
	vendorethnicity VARCHAR(2) ENCODE zstd,
	occupationid VARCHAR(12) ENCODE zstd,
	fulfillmentflag VARCHAR(1) ENCODE zstd,
	emailaddress VARCHAR(60) ENCODE zstd,
	emailmatchindicator VARCHAR(1) ENCODE zstd,
	emailsource VARCHAR(2) ENCODE zstd,
	residentialbusinessindicator VARCHAR(1) ENCODE zstd,
	employername VARCHAR(30) ENCODE zstd,
	familyid VARCHAR(12) ENCODE zstd,
	individualid VARCHAR(12) ENCODE zstd,
	abinumber VARCHAR(9) ENCODE zstd,
	siccode VARCHAR(6) ENCODE bytedict,
	scf VARCHAR(3) ENCODE zstd,
	licensestatuscode VARCHAR(1) ENCODE zstd,
	md5_upper VARCHAR(32) ENCODE zstd,
	md5_lower VARCHAR(32) ENCODE zstd,
	state_donotcall_flag VARCHAR(1) ENCODE zstd,
	ce_education_model VARCHAR(1) ENCODE zstd,
	ce_education_model_desc	VARCHAR(31) ENCODE zstd,
	individual_mc VARCHAR(17) ENCODE zstd,
	company_mc VARCHAR(17) ENCODE zstd,
	email_marketable VARCHAR(1) ENCODE zstd,
	email_reputation_risk VARCHAR(1) ENCODE zstd,
	BV_flag VARCHAR(1) ENCODE zstd,
	id BIGINT UNIQUE distkey sortkey ENCODE az64,
	zipradius VARCHAR(1) ENCODE lzo,
	georadius VARCHAR(1) ENCODE lzo,
	fullname VARCHAR(50) ENCODE zstd,
	zipfull VARCHAR(9) ENCODE zstd,
	emailavailable VARCHAR(1) ENCODE zstd,
	fax VARCHAR(20) ENCODE lzo,
	phone VARCHAR(20) ENCODE zstd,
	phone_available VARCHAR(1) ENCODE zstd,
	domain_name VARCHAR(60) ENCODE zstd,
	company VARCHAR(30) ENCODE lzo,
	addressline2 VARCHAR(50) ENCODE lzo,
	listid integer ENCODE az64,
	productcode VARCHAR(2) ENCODE zstd,
	listtype VARCHAR(1) ENCODE zstd,
	permissiontype VARCHAR(1) ENCODE zstd,
	dropflag VARCHAR(1) ENCODE lzo,
	industrytitlecode VARCHAR(2) ENCODE zstd,
	occupationtitlecode VARCHAR(3) ENCODE bytedict,
	specialtytitlecode VARCHAR(3) ENCODE bytedict,
	hardkey VARCHAR(55) ENCODE zstd,
	flongitude float ENCODE zstd,
	flatitude float ENCODE zstd,
	statecountyname VARCHAR(60) ENCODE zstd,
	statecity VARCHAR(50) ENCODE zstd,
	statecityzip VARCHAR(50) ENCODE zstd,
	statecountycode VARCHAR(5) ENCODE zstd,
	statecityscf VARCHAR(50) ENCODE zstd,
	county_code_description VARCHAR(100) ENCODE zstd,
	donotcallflag VARCHAR(1) ENCODE zstd,
	individual_mc_wb VARCHAR(17) ENCODE zstd,
	company_mc_wb VARCHAR(17) ENCODE zstd
);

insert into temp_{tablename1}
(
  lastname,
  firstname,
  middlename,
  title,
  gender,
  addressline1,
  city,
  state,
  zip,
  zipfour,
  carrierroutecode,
  delivery_point_bar_code,
  industrytitle,
  occupationtitle,
  specialtytitle,
  censusstatecode,
  censuscountycode,
  censustract,
  censusblockgroup,
  highrise,
  yearlicensed,
  licensestate,
  licensenumber,
  enhancedphone,
  area_code,
  phone_number,
  time_zone,
  numberofchildren,
  childrenbygender0,
  childrenbygender1,
  childrenbygender2,
  childrenbygender3,
  childrenbygender4,
  childrenbygender5,
  childrenbygender6,
  childrenbygender7,
  childrenbygender8,
  childrenbygender9,
  childrenbygender10,
  childrenbygender11,
  childrenbygender12,
  childrenbygender13,
  childrenbygender14,
  childrenbygender15,
  childrenbygender16,
  childrenbygender17,
  length_of_residence,
  own_rent_indicator,
  find_income,
  activebankcard,
  donor_ever,
  home_value_code,
  cat_owner,
  dog_owner,
  mail_responsive,
  mail_responsive_previous,
  mail_order,
  marital_status,
  health_contributor,
  political_contributor,
  religious_contirbutor,
  personal_computer,
  AGE,
  latitude,
  longitude,
  geo_match_code,
  cbsa_code,
  cbsa_code_description,
  cbsa_level_description,
  county_code,
  mailscore,
  housenumber,
  streetpredirectional,
  streetname,
  streetsuffix,
  streetpostdirectional,
  unittype,
  unitnumber,
  vendorethnicity,
  occupationid,
  fulfillmentflag,
  emailaddress,
  emailmatchindicator,
  emailsource,
  residentialbusinessindicator,
  employername,
  familyid,
  individualid,
  abinumber,
  siccode,
  scf,
  licensestatuscode,
  md5_upper,
  md5_lower,
  state_donotcall_flag,
  ce_education_model,
  ce_education_model_desc,
  individual_mc,
  company_mc,
  email_marketable,
  email_reputation_risk,
  bv_flag,
  id,
  zipradius,
  georadius,
  fullname,
  zipfull,
  emailavailable,
  fax,
  phone,
  phone_available,
  domain_name,
  company,
  addressline2,
  listid,
  productcode,
  listtype,
  permissiontype,
  dropflag,
  industrytitlecode,
  occupationtitlecode,
  specialtytitlecode,
  hardkey,
  flongitude,
  flatitude,
  statecountyname,
  statecity,
  statecityzip,
  statecountycode,
  statecityscf,
  county_code_description,
  donotcallflag,
  individual_mc_wb,
  company_mc_wb
)
SELECT
    a.lastname AS lastname,
    a.firstname AS firstname,
    a.middlename AS middlename,
    a.title AS title,
    CASE WHEN a.gender IS NULL THEN '' ELSE a.gender END AS gender,
    a.addressline1 AS addressline1,
    a.city AS city,
    a.state AS state,
    a.zip AS zip,
    a.zipfour AS zipfour,
    a.carrierroutecode AS carrierroutecode,
    a.delivery_point_bar_code AS delivery_point_bar_code,
    a.industrytitle AS industrytitle,
    a.occupationtitle AS occupationtitle,
    a.specialtytitle AS specialtytitle,
    a.censusstatecode AS censusstatecode,
    a.censuscountycode AS censuscountycode,
    a.censustract AS censustract,
    a.censusblockgroup AS censusblockgroup,
    a.highrise AS highrise,
    a.yearlicensed AS yearlicensed,
    a.licensestate AS licensestate,
    a.licensenumber AS licensenumber,
    a.enhancedphone AS enhancedphone,
    a.area_code AS area_code,
    a.phone_number AS phone_number,
    a.time_zone AS time_zone,
    a.numberofchildren AS numberofchildren,
    a.childrenbygender0 AS childrenbygender0 ,
    a.childrenbygender1 AS childrenbygender1 ,
    a.childrenbygender2 AS childrenbygender2 ,
    a.childrenbygender3 AS childrenbygender3 ,
    a.childrenbygender4 AS childrenbygender4 ,
    a.childrenbygender5 AS childrenbygender5,
    a.childrenbygender6 AS childrenbygender6,
    a.childrenbygender7 AS childrenbygender7,
    a.childrenbygender8 AS childrenbygender8,
    a.childrenbygender9 AS childrenbygender9,
    a.childrenbygender10 AS childrenbygender10 ,
    a.childrenbygender11 AS childrenbygender11 ,
    a.childrenbygender12 AS childrenbygender12 ,
    a.childrenbygender13 AS childrenbygender13 ,
    a.childrenbygender14 AS childrenbygender14 ,
    a.childrenbygender15 AS childrenbygender15 ,
    a.childrenbygender16 AS childrenbygender16 ,
    a.childrenbygender17 AS childrenbygender17 ,
    a.length_of_residence AS length_of_residence ,
    a.own_rent_indicator AS own_rent_indicator ,
    a.find_income AS find_income ,
    a.activebankcard AS activebankcard,
    a.donor_ever AS donor_ever,
    a.home_value_code AS home_value_code,
    a.cat_owner AS cat_owner,
    a.dog_owner AS dog_owner,
    a.mail_responsive AS mail_responsive,
    a.mail_responsive_previous AS mail_responsive_previous,
    a.mail_order AS mail_order,
    a.marital_status AS marital_status ,
    a.health_contributor AS health_contributor ,
    a.political_contributor AS political_contributor ,
    a.religious_contirbutor AS religious_contirbutor ,
    a.personal_computer AS personal_computer ,
    a.age AS age ,
    a.latitude AS latitude ,
    a.longitude AS longitude ,
    a.geo_match_code AS geo_match_code ,
    a.cbsa_code AS cbsa_code ,
    a.cbsa_code_description AS cbsa_code_description ,
    a.cbsa_level_description AS cbsa_level_description ,
    a.county_code AS county_code ,
    a.mailscore AS mailscore ,
    a.housenumber AS housenumber ,
    a.streetpredirectional AS streetpredirectional ,
    a.streetname AS streetname ,
    a.streetsuffix AS streetsuffix ,
    a.streetpostdirectional AS streetpostdirectional ,
    a.unittype AS unittype ,
    a.unitnumber AS unitnumber ,
    a.vendorethnicity AS vendorethnicity ,
    a.occupationid AS occupationid ,
    a.fulfillmentflag AS fulfillmentflag ,
    a.emailaddress AS emailaddress ,
    a.emailmatchindicator AS emailmatchindicator ,
    a.emailsource AS emailsource,
    a.residentialbusinessindicator AS residentialbusinessindicator ,
    a.employername AS employername ,
    a.familyid AS familyid ,
    a.individualid AS individualid ,
    a.abinumber AS abinumber ,
    a.siccode AS siccode,
    a.scf AS scf,
    a.licensestatuscode AS licensestatuscode,
    a.md5_upper AS md5_upper,
    a.md5_lower AS md5_lower,
    a.state_donotcall_flag AS state_donotcall_flag,
    a.ce_education_model AS ce_education_model,
    a.ce_education_model_desc AS ce_education_model_desc,
    GetMatchCode (a.firstname, CASE WHEN (a.lastname = '' OR a.lastname IS NULL) THEN a.company ELSE a.lastname END, a.addressline1, a.zip, '' ,'I') AS individual_mc,
    GetMatchCode (a.firstname, a.lastname, a.addressline1, a.zip, CASE WHEN (a.company = '' OR a.company IS NULL) THEN a.lastname ELSE a.company END,'C')  AS company_mc,
	a.email_marketable AS email_marketable,
	a.email_reputation_risk AS email_reputation_risk,
	a.BV_flag AS BV_flag,
    a.id AS id,
    NULL AS zipradius,
    NULL AS georadius,
    a.firstname + ' ' + a.lastname AS fullname,
    a.zip + a.zipfour AS zipfull,
    CASE WHEN LTRIM(RTRIM(a.emailaddress)) = '' THEN 'N' 
         ELSE 'Y' END AS emailavailable,
    NULL AS fax,
    a.area_code + a.phone_number AS phone,
    CASE WHEN LTRIM(RTRIM(a.phone_number)) = '' THEN 'N' 
         ELSE 'Y' END AS phone_available,
    CASE WHEN CHARINDEX('@', a.emailaddress) > 1 THEN RIGHT(a.emailaddress, CHARINDEX('@', REVERSE(a.emailaddress))-1) 
         ELSE '' END AS domain_name,
    NULL AS company,
    NULL AS addressline2,
    13884 AS listid,
    '' AS productcode,
    'Z' AS listtype,
    'R' AS permissiontype,
    NULL AS dropflag,
    CASE LTRIM(RTRIM(a.industrytitle)) WHEN 'ACCOUNTING' THEN '01' 
                                       WHEN 'AGRICULTURE' THEN '02' 
                                       WHEN 'ARCHITECTURE' THEN '03' 
                                       WHEN 'BUSINESS SERVICES' THEN '04' 
                                       WHEN 'CONSTRUCTION' THEN '05' 
                                       WHEN 'EDUCATION' THEN '06' 
                                       WHEN 'ELECTRIC, GAS, SANITARY SVCS' THEN '07' 
                                       WHEN 'ENGINEERING' THEN '08' 
                                       WHEN 'ENTERTAINMENT' THEN '09' 
                                       WHEN 'FINANCE' THEN '10' 
                                       WHEN 'HEALTH CARE' THEN '11' 
                                       WHEN 'INSURANCE' THEN '12' 
                                       WHEN 'LEGAL' THEN '13' 
                                       WHEN 'MANAGEMENT/CONSULTING' THEN '14' 
                                       WHEN 'PERSONAL SERVICES' THEN '15' 
                                       WHEN 'REAL ESTATE' THEN '16' 
                                       WHEN 'RECREATION' THEN '17' 
                                       WHEN 'REPAIR' THEN '18' 
                                       WHEN 'RETAIL' THEN '19' 
                                       WHEN 'SOCIAL SERVICES' THEN '20' 
                                       WHEN 'TESTING SERVICES' THEN '21' 
                                       WHEN 'TRANSPORTATION' THEN '22' 
                                       WHEN 'VETERINARY' THEN '23' 
                                       ELSE '' END AS industrytitlecode,
    CASE LTRIM(RTRIM(a.occupationtitle)) WHEN 'ACCOUNTANT' THEN '01'
                                        WHEN 'ACUPUNCTURIST' THEN '02'
                                        WHEN 'AGENT' THEN '03'
                                        WHEN 'ANESTHESIOLOGIST ASSISTANT' THEN '04'
                                        WHEN 'ANIMAL BREEDER' THEN '05'
                                        WHEN 'ARCHITECT' THEN '06'
                                        WHEN 'ASBESTOS REMOVAL' THEN '07'
                                        WHEN 'ATHLETIC TRAINER' THEN '08'
                                        WHEN 'ATTORNEY' THEN '09'
                                        WHEN 'AUCTIONEER' THEN '10'
                                        WHEN 'AUDIOLOGIST' THEN '11'
                                        WHEN 'AUDIOLOGY ASSISTANT' THEN '12'
                                        WHEN 'AUDIO-VISUAL SPECIALIST' THEN '13'
                                        WHEN 'AUTO SALES' THEN '14'
                                        WHEN 'BAIL BONDSMAN' THEN '15'
                                        WHEN 'BARBER' THEN '16'
                                        WHEN 'BOAT SALES' THEN '17'
                                        WHEN 'CEMETERY SALES PERSON' THEN '18'
                                        WHEN 'CHILD CARE WORKER' THEN '19'
                                        WHEN 'CHIROPRACTIC ASSISTANT' THEN '20'
                                        WHEN 'CHIROPRACTOR' THEN '21'
                                        WHEN 'COLLECTIONS/REPOSSESSOR' THEN '22'
                                        WHEN 'CONSERVATION (SOIL EROSION, CROP)' THEN '23'
                                        WHEN 'CONTRACTOR' THEN '24'
                                        WHEN 'CONTROL TOWER OPERATOR' THEN '25'
                                        WHEN 'COSMETOLOGIST' THEN '26'
                                        WHEN 'COUNSELOR' THEN '27'
                                        WHEN 'DENTAL HYGENIST' THEN '28'
                                        WHEN 'DENTIST' THEN '29'
                                        WHEN 'DENTIST ASSISTANT' THEN '30'
                                        WHEN 'DETECTIVES / PRIVATE EYE' THEN '31'
                                        WHEN 'DIETICIAN/NUTRITIONIST' THEN '32'
                                        WHEN 'DISPATCHER' THEN '33'
                                        WHEN 'DOCTORS/PHYSICIANS' THEN '34'
                                        WHEN 'EDUCATIONAL ADMINISTRATOR' THEN '35'
                                        WHEN 'ELECTRICAL & ELCTRONIC ENGNR TECHNICIANS' THEN '36'
                                        WHEN 'ELECTRICIAN' THEN '37'
                                        WHEN 'ELECTROLOGIST' THEN '38'
                                        WHEN 'ELEVATOR INSTALL/REPAIR' THEN '39'
                                        WHEN 'EMPLOYMENT CONSULTANTS' THEN '40'
                                        WHEN 'EMT / PARAMEDIC' THEN '41'
                                        WHEN 'ENGINEER INTERN' THEN '42'
                                        WHEN 'ENGINEER PROFESSIONAL' THEN '43' 
                                        WHEN 'ESTHETICIAN' THEN '44' 
                                        WHEN 'EYE/VISION' THEN '45' 
                                        WHEN 'FIDUCIARY/TRUSTEE' THEN '46' 
                                        WHEN 'FIRE PROTECTION EQUIP AND SERVICES' THEN '47' 
                                        WHEN 'FLIGHT AIRCRAFT INSTRUCTION' THEN '48' 
                                        WHEN 'FLIGHT ENGINEER' THEN '49' 
                                        WHEN 'FLIGHT NAVIGATOR' THEN '50' 
                                        WHEN 'FOOD SERVICE' THEN '51' 
                                        WHEN 'FORESTER' THEN '52' 
                                        WHEN 'FUNERAL ATTENDANTS' THEN '53' 
                                        WHEN 'FUNERAL DIRECTORS' THEN '54' 
                                        WHEN 'FUNERAL EMBALMERS' THEN '55' 
                                        WHEN 'GEOLOGIST' THEN '56' 
                                        WHEN 'GLASS INSTALLER AUTO' THEN '57' 
                                        WHEN 'GRAPHIC DESIGNER' THEN '58' 
                                        WHEN 'GUARDIAN / CONSERVATOR' THEN '59' 
                                        WHEN 'HAIR STYLIST' THEN '60' 
                                        WHEN 'HANDLERS AND BLASTERS' THEN '61' 
                                        WHEN 'HAZMAT (CHEM, LEAD, ASBESTOS)' THEN '62' 
                                        WHEN 'HAZMAT TECHNICIAN' THEN '63' 
                                        WHEN 'HEARING AID FITTING, SALES & REPAIR' THEN '64' 
                                        WHEN 'HEATING VENTILATION AIR CONDITIONING TECH' THEN '65' 
                                        WHEN 'HOME & COMMERCIAL BLDG INSPECTION' THEN '66' 
                                        WHEN 'HOME HEALTH AID' THEN '67' 
                                        WHEN 'HOME IMPROVEMENT SALES' THEN '68' 
                                        WHEN 'HORSE RACING' THEN '69' 
                                        WHEN 'HUNTING/FISHING GUIDE' THEN '70' 
                                        WHEN 'HYPNOTIST' THEN '71' 
                                        WHEN 'INSURANCE ADJUSTER' THEN '72' 
                                        WHEN 'INSURANCE ADVISOR/CONSULTANT' THEN '73' 
                                        WHEN 'INSURANCE AGENT' THEN '74' 
                                        WHEN 'INTERIOR DESIGNER' THEN '75' 
                                        WHEN 'INTERPRETERS (LANGUAGE & DEAF)' THEN '76' 
                                        WHEN 'LANDSCAPE ARCHITECT / DESIGN' THEN '77'
                                        WHEN 'LEGAL SERVICE CONTRACT' THEN '78'
                                        WHEN 'LIBRARIAN / ARCHIVIST' THEN '79'
                                        WHEN 'LIQUEFIED PETROLEUM GAS DEALER' THEN '80'
                                        WHEN 'LOBBYIST' THEN '81'
                                        WHEN 'LOCKSMITH' THEN '82'
                                        WHEN 'MANICURIST/PEDICURIST' THEN '83'
                                        WHEN 'MATCHMAKERS' THEN '84'
                                        WHEN 'MECHANIC' THEN '85'
                                        WHEN 'MEDICAL APPLIANCE TECH (GAS & VACUUM)' THEN '86'
                                        WHEN 'MEDICAL MANAGERS' THEN '87'
                                        WHEN 'MEDICAL NUCLEAR TECHNICIAN' THEN '88'
                                        WHEN 'MEDICAL PHYSICIST' THEN '89'
                                        WHEN 'MIDWIFE (NOT RN)' THEN '90'
                                        WHEN 'MOTOR VEHICLE MECHANIC' THEN '91'
                                        WHEN 'NATUROPATHIC/HOMEOPATHIC PHYSICIAN OR COUNSELOR' THEN '92'
                                        WHEN 'NURSE ASSIST CERT (CNA)' THEN '93'
                                        WHEN 'NURSE PRACTICAL (LPN)' THEN '94'
                                        WHEN 'NURSE REGISTERED (RN)' THEN '95'
                                        WHEN 'NURSING HOME ADMINISTRATOR' THEN '96'
                                        WHEN 'NURSING HOME AIDE' THEN '97'
                                        WHEN 'OSTEOPATH' THEN '98'
                                        WHEN 'PARACHUTE RIGGER' THEN '99'
                                        WHEN 'PARTS SALES' THEN '100'
                                        WHEN 'PERFUSIONIST' THEN '101'
                                        WHEN 'PEST CONTROL' THEN '102'
                                        WHEN 'PHARMACIST' THEN '103'
                                        WHEN 'PHARMACIST INTERN' THEN '104'
                                        WHEN 'PHARMACY TECHNICIAN' THEN '105'
                                        WHEN 'PILOT - AIRPLANE' THEN '106'
                                        WHEN 'PILOT - MARINE (SHIP)' THEN '107'
                                        WHEN 'PLUMBER' THEN '108'
                                        WHEN 'PODIATRIST' THEN '109'
                                        WHEN 'PODIATRY ASSISTANT' THEN '110'
                                        WHEN 'POLYGRAPH EXAMINERS' THEN '111'
                                        WHEN 'PROBATION WORKER' THEN '112'
                                        WHEN 'PROSHETIST/ORTHOTIST' THEN '113'
                                        WHEN 'PSYCHIATRIC TECHNICIAN' THEN '114'
                                        WHEN 'PSYCHOLOGIST' THEN '115'
                                        WHEN 'PSYCHOLOGIST ASSISTANT' THEN '116'
                                        WHEN 'PSYCHOTHERAPIST' THEN '117'
                                        WHEN 'RADIOLOGICAL TECHNICIAN/TECHNOLOGIST' THEN '118'
                                        WHEN 'REAL ESTATE ABSTRACTOR/TITLE AGENT' THEN '119'
                                        WHEN 'REAL ESTATE APPRAISAL' THEN '120'
                                        WHEN 'REAL ESTATE MANAGEMENT' THEN '121'
                                        WHEN 'REAL ESTATE SALES' THEN '122'
                                        WHEN 'RECYCLING & RECLAMATION WORKERS' THEN '123'
                                        WHEN 'SALESPERSON' THEN '124'
                                        WHEN 'SANITARIAN (HEALTH)' THEN '125'
                                        WHEN 'SCIENTIST - CONSULTING' THEN '126'
                                        WHEN 'SECURITY ALARM MONITOR/TECH' THEN '127'
                                        WHEN 'SECURITY GUARD' THEN '128'
                                        WHEN 'SECURITY TRAINING INSTRUCTOR' THEN '129'
                                        WHEN 'SHEET METAL WORKER' THEN '130'
                                        WHEN 'SPEECH / LANGUAGE ASSISTANT' THEN '131'
                                        WHEN 'SPEECH / LANGUAGE PATHOLOGIST' THEN '132'
                                        WHEN 'SPORTS AGENTS' THEN '133'
                                        WHEN 'SPORTS OFFICIALS' THEN '134'
                                        WHEN 'SPORTS PROF ATHLETES' THEN '135'
                                        WHEN 'SPORTS PROMOTERS' THEN '136'
                                        WHEN 'STATIONARY ENG, BOILER INSTALLERS & OPERATORS' THEN '137'
                                        WHEN 'STORAGE TANK WORKER' THEN '138'
                                        WHEN 'SURVEYOR / MAPPER' THEN '139'
                                        WHEN 'TALENT AGENT' THEN '140'
                                        WHEN 'TATTOOISTS, BODY PIERCERS' THEN '141'
                                        WHEN 'TAX CONSULTANTS' THEN '142'
                                        WHEN 'TAXIDERMIST' THEN '143'
                                        WHEN 'TEACHER' THEN '144'
                                        WHEN 'TECHNICIAN' THEN '145'
                                        WHEN 'TECHNICIANS - LABORATORY' THEN '146'
                                        WHEN 'TECHNOLOGIST' THEN '147'
                                        WHEN 'TELECOMMUNICATIONS MANAGER' THEN '148'
                                        WHEN 'TELEVISION REPAIR' THEN '149'
                                        WHEN 'THERAPIST' THEN '150'
                                        WHEN 'TOWING OPERATOR' THEN '151'
                                        WHEN 'TRANSCRIBERS - COURT' THEN '152'
                                        WHEN 'VETERINARIAN' THEN '153'
                                        WHEN 'VETERINARIAN ASSISTANT' THEN '154'
                                        WHEN 'VINTNER' THEN '155'
                                        WHEN 'WATER / WASTE TREATMENT OPERATOR' THEN '156'
                                        WHEN 'WATER WELL DRILLERS, PUMP INSTALLERS' THEN '157'
                                        WHEN 'WEIGHING DEVICES' THEN '158'
                                        WHEN 'X-RAY ASSISTANT' THEN '159'
                                        ELSE ''END AS occupationtitlecode,
    CASE LTRIM(RTRIM(a.specialtytitle)) WHEN 'ACCESSIBILITY SPECIALIST' THEN '01'
                                        WHEN 'ACCIDENT/HEALTH/SICKNESS' THEN '02'
                                        WHEN 'ACUTE CARE PRACTITIONER' THEN '03'
                                        WHEN 'ADDICTION' THEN '04'
                                        WHEN 'ADDICTION COUNSELOR' THEN '05'
                                        WHEN 'ADJUSTER/APPRAISER/INVESTIGATOR' THEN '06'
                                        WHEN 'ADMINISTRATION' THEN '07'
                                        WHEN 'ADOLESCENT' THEN '08'
                                        WHEN 'ADULT CARE' THEN '09'
                                        WHEN 'ADULTS' THEN '10'
                                        WHEN 'ADVANCED PRACTICE' THEN '11'
                                        WHEN 'ADVISOR/CONSULTANT/BROKER' THEN '12'
                                        WHEN 'AERONAUTICAL' THEN '13'
                                        WHEN 'AEROSPACE' THEN '14'
                                        WHEN 'AGING/ADULT' THEN '15'
                                        WHEN 'AGRICULTURAL' THEN '16'
                                        WHEN 'AIDS' THEN '17'
                                        WHEN 'AIRCRAFT' THEN '18'
                                        WHEN 'AIRPLANE' THEN '19'
                                        WHEN 'ALLERGY' THEN '20'
                                        WHEN 'AMBULATORY CARE' THEN '21'
                                        WHEN 'ANESTHESIOLOGY/ANESTHETIST' THEN '22'
                                        WHEN 'ANNUITY (VARIABLE)' THEN '23'
                                        WHEN 'APPRAISERS' THEN '24'
                                        WHEN 'ARCHITECTURAL' THEN '25'
                                        WHEN 'ASBESTOS' THEN '26'
                                        WHEN 'ASSOCIATIONS' THEN '27'
                                        WHEN 'ATHLETIC' THEN '28'
                                        WHEN 'AUCTION' THEN '29'
                                        WHEN 'AUTO' THEN '30'
                                        WHEN 'AVIATION MEDICAL EXAMINER' THEN '31'
                                        WHEN 'BEHAVIORAL' THEN '32'
                                        WHEN 'BIOMEDICAL' THEN '33'
                                        WHEN 'BOAT' THEN '34'
                                        WHEN 'BUSINESS LINES (PROPERTY, CASUALTY)' THEN '35'
                                        WHEN 'CARDIOLOGY' THEN '36'
                                        WHEN 'CARDIOVASCULAR/CARDIOLOGY' THEN '37'
                                        WHEN 'CEMETARY' THEN '38'
                                        WHEN 'CHEMICAL' THEN '39'
                                        WHEN 'CHILD CARE' THEN '40'
                                        WHEN 'CHRONIC/LONG TERM CARE' THEN '41'
                                        WHEN 'CIVIL' THEN '42'
                                        WHEN 'CLINICAL' THEN '43'
                                        WHEN 'CLINICAL NURSE' THEN '44'
                                        WHEN 'COLO/RECTAL' THEN '45'
                                        WHEN 'COMMUNITY HEALTH' THEN '46'
                                        WHEN 'COMPUTER/COMMUNICATIONS' THEN '47'
                                        WHEN 'CONSTRUCTION' THEN '48'
                                        WHEN 'CONSULTING' THEN '49'
                                        WHEN 'CONTROL SYSTEMS' THEN '50'
                                        WHEN 'CREDIT INSURANCE' THEN '51'
                                        WHEN 'CRITICAL CARE' THEN '52'
                                        WHEN 'CROP' THEN '53'
                                        WHEN 'CYTOPATHOLOGY' THEN '54'
                                        WHEN 'DERMATOLOGY' THEN '55'
                                        WHEN 'DESIGN' THEN '56'
                                        WHEN 'DIALYSIS' THEN '57'
                                        WHEN 'DRUG / ALCOHOL' THEN '58'
                                        WHEN 'EAR/NOSE/THROAT' THEN '59'
                                        WHEN 'EDUCATION/SCHOOL' THEN '60'
                                        WHEN 'ELECTRICAL' THEN '61'
                                        WHEN 'ELECTRICIAN APPRENTICE / HELPER' THEN '62'
                                        WHEN 'ELECTRICIAN JOURNEYMAN' THEN '63'
                                        WHEN 'ELECTRICIAN MASTER' THEN '64'
                                        WHEN 'ELEMENTARY' THEN '65'
                                        WHEN 'ELEVATOR' THEN '66'
                                        WHEN 'EMERGENCY CARE' THEN '67'
                                        WHEN 'EMERGENCY MEDICINE' THEN '68'
                                        WHEN 'ENDOCRINOLOGY' THEN '69'
                                        WHEN 'ENVIRONMENTAL' THEN '70'
                                        WHEN 'EXPLOSIVES' THEN '71'
                                        WHEN 'EYE / EAR / NOSE / THROAT' THEN '72'
                                        WHEN 'FAMILY CARE' THEN '73'
                                        WHEN 'FAMILY PLANNING' THEN '74'
                                        WHEN 'FAMILY PRACTICE' THEN '75'
                                        WHEN 'FIRE' THEN '76'
                                        WHEN 'FIRE PROTECTION' THEN '77'
                                        WHEN 'FIREARMS/BATON/MARTIAL ARTS' THEN '78'
                                        WHEN 'FISHING, HUNTING' THEN '79'
                                        WHEN 'FORENSIC MEDICINE' THEN '80'
                                        WHEN 'FORESTRY' THEN '81'
                                        WHEN 'GAS' THEN '82'
                                        WHEN 'GASTROENTEROLOGY' THEN '83'
                                        WHEN 'GENERAL CONSTRUCTION' THEN '84'
                                        WHEN 'GENERAL PRACTICE' THEN '85'
                                        WHEN 'GENETIC' THEN '86'
                                        WHEN 'GENETICS' THEN '87'
                                        WHEN 'GEOLOGICAL' THEN '88'
                                        WHEN 'GEOLOGY' THEN '89'
                                        WHEN 'GEOTECHNICAL' THEN '90'
                                        WHEN 'GERIATRIC CARE' THEN '91'
                                        WHEN 'GERIATRICS' THEN '92'
                                        WHEN 'HAZARDOUS WASTE' THEN '93'
                                        WHEN 'HEATING/AC' THEN '94'
                                        WHEN 'HEATING/AIR/VENTILATION' THEN '95'
                                        WHEN 'HEMATOLOGY' THEN '96'
                                        WHEN 'HEPATOLOGY' THEN '97'
                                        WHEN 'HOLISTIC' THEN '98'
                                        WHEN 'HOME HEALTH CARE' THEN '99'
                                        WHEN 'HOME INSPECTION' THEN '100'
                                        WHEN 'HOSPICE' THEN '101'
                                        WHEN 'HOSPITALIST' THEN '102'
                                        WHEN 'HYPERBARIC MEDICINE' THEN '103'
                                        WHEN 'IMMUNOLOGY' THEN '104'
                                        WHEN 'INDUSTRIAL' THEN '105'
                                        WHEN 'INFECTION CONTROL' THEN '106'
                                        WHEN 'INFECTIOUS DISEASE' THEN '107'
                                        WHEN 'INFORMATICS' THEN '108'
                                        WHEN 'INTERNAL MEDICINE' THEN '109'
                                        WHEN 'IV CERTIFICATION' THEN '110'
                                        WHEN 'LAND PLANNING' THEN '111'
                                        WHEN 'LANDSCAPE SVCS' THEN '112'
                                        WHEN 'LEGAL' THEN '113'
                                        WHEN 'LIBRARY' THEN '114'
                                        WHEN 'LIFE/FIXED ANNUITY AGENTS' THEN '115'
                                        WHEN 'LIMITED LINES' THEN '116'
                                        WHEN 'LUMBER/BUILDING' THEN '117'
                                        WHEN 'MANAGER' THEN '118'
                                        WHEN 'MANUFACTURED HOUSING' THEN '119'
                                        WHEN 'MANUFACTURING' THEN '120'
                                        WHEN 'MARINE' THEN '121'
                                        WHEN 'MARINE (HAZARDS, COLLECTIONS, LOSS)' THEN '122'
                                        WHEN 'MARRIAGE & FAMILY COUNSELORS' THEN '123'
                                        WHEN 'MASSAGE' THEN '124'
                                        WHEN 'MASSAGE THERAPY' THEN '125'
                                        WHEN 'MATERNAL / CHILD' THEN '126'
                                        WHEN 'MECHANICAL' THEN '127'
                                        WHEN 'MEDICAL' THEN '128'
                                        WHEN 'MEDICAL / SURGICAL' THEN '129'
                                        WHEN 'MEDICINE ASST' THEN '130'
                                        WHEN 'MENTAL HEALTH COUNSELOR' THEN '131'
                                        WHEN 'METAL' THEN '132'
                                        WHEN 'MICROBIOLOGY' THEN '133'
                                        WHEN 'MIDWIFE' THEN '134'
                                        WHEN 'MINING' THEN '135'
                                        WHEN 'MUNICIPAL' THEN '136'
                                        WHEN 'NEO/NATAL' THEN '137'
                                        WHEN 'NEONATAL' THEN '138'
                                        WHEN 'NEPHROLOGY' THEN '139'
                                        WHEN 'NEUROLOGY' THEN '140'
                                        WHEN 'NEUROPATHOLOGY' THEN '141'
                                        WHEN 'NEUROPSYCHOLOGIST' THEN '142'
                                        WHEN 'NEUROSCIENCE' THEN '143'
                                        WHEN 'NUCLEAR' THEN '144'
                                        WHEN 'NUCLEAR MEDICINE' THEN '145'
                                        WHEN 'NURSE ANESTHESIOLOGIST' THEN '146'
                                        WHEN 'NURSE ANESTHETIST' THEN '147'
                                        WHEN 'NURSE PRACTITIONER' THEN '148'
                                        WHEN 'NUTRITION' THEN '149'
                                        WHEN 'OB / GYN' THEN '150'
                                        WHEN 'OB/GYN' THEN '151'
                                        WHEN 'OCCUPATIONAL' THEN '152'
                                        WHEN 'OCCUPATIONAL HEALTH' THEN '153'
                                        WHEN 'OCCUPATIONAL THERAPY ASST' THEN '154'
                                        WHEN 'OCULARIST' THEN '155'
                                        WHEN 'ONCOLOGY' THEN '156'
                                        WHEN 'ONCOLOGY PEDIATRIC' THEN '157'
                                        WHEN 'OPHTHALMLIC ASSISTANT' THEN '158'
                                        WHEN 'OPHTHALMOLOGIST' THEN '159'
                                        WHEN 'OPHTHALMOLOGY (OD)' THEN '160'
                                        WHEN 'OPTHALMOLOGY' THEN '161'
                                        WHEN 'OPTICIAN' THEN '162'
                                        WHEN 'OPTICIAN APPRENTICE/ASSISTANT' THEN '163'
                                        WHEN 'OPTOMETRIST (OD)' THEN '164'
                                        WHEN 'ORTHODONTIST' THEN '165'
                                        WHEN 'ORTHOPAEDICS' THEN '166'
                                        WHEN 'ORTHOPEDICS' THEN '167'
                                        WHEN 'ORTHOPTISTS' THEN '168'
                                        WHEN 'OWNER' THEN '169'
                                        WHEN 'PAIN MANAGEMENT' THEN '170'
                                        WHEN 'PASTORAL' THEN '171'
                                        WHEN 'PATENT' THEN '172'
                                        WHEN 'PATHOLOGY' THEN '173'
                                        WHEN 'PEDIATRICS' THEN '174'
                                        WHEN 'PERINATAL' THEN '175'
                                        WHEN 'PERI-OPERATIVE' THEN '176'
                                        WHEN 'PERSONAL (HOME, AUTO, RENTER, PERS LIABILITY)' THEN '177'
                                        WHEN 'PETROLEUM' THEN '178'
                                        WHEN 'PHARMACOLOGY' THEN '179'
                                        WHEN 'PHYSICAL (LICENSED)' THEN '180'
                                        WHEN 'PHYSICAL THERAPY ASST (PTA)' THEN '181'
                                        WHEN 'PHYSICIAN''S ASSISTANT' THEN '182'
                                        WHEN 'PLASTIC SURGERY' THEN '183'
                                        WHEN 'PLUMBER APPRENTICE' THEN '184'
                                        WHEN 'PLUMBER JOURNEYMAN' THEN '185'
                                        WHEN 'PLUMBER MASTER' THEN '186'
                                        WHEN 'PLUMBING' THEN '187'
                                        WHEN 'POLYGRAPH' THEN '188'
                                        WHEN 'POST SECONDARY' THEN '189'
                                        WHEN 'PRESCHOOL' THEN '190'
                                        WHEN 'PRESCRIPTIVE AUTHORITY' THEN '191'
                                        WHEN 'PROBATION SERVICES' THEN '192'
                                        WHEN 'PROSTHODONTIST (TEETH)' THEN '193'
                                        WHEN 'PSYCHIATRY' THEN '194'
                                        WHEN 'PSYCHIATRY ADULT' THEN '195'
                                        WHEN 'PSYCHIATRY CHILD' THEN '196'
                                        WHEN 'PSYCHIATRY FAMILY' THEN '197'
                                        WHEN 'PSYCHIATRY GENERAL' THEN '198'
                                        WHEN 'PUBLIC HEALTH' THEN '199'
                                        WHEN 'PUBLIC WEIGHER' THEN '200'
                                        WHEN 'PULMONOLOGY' THEN '201'
                                        WHEN 'RADIOLOGY' THEN '202'
                                        WHEN 'RECYCLING' THEN '203'
                                        WHEN 'REHABILITATION' THEN '204'
                                        WHEN 'REPRODUCTIVE HEALTH' THEN '205'
                                        WHEN 'RESPIRATORY' THEN '206'
                                        WHEN 'RHEUMATOLOGY' THEN '207'
                                        WHEN 'SAFETY MANAGER' THEN '208'
                                        WHEN 'SANITARY' THEN '209'
                                        WHEN 'SCHOOL NURSE' THEN '210'
                                        WHEN 'SECONDARY' THEN '211'
                                        WHEN 'SLEEP MEDICINE' THEN '212'
                                        WHEN 'SOCIAL WORKERS' THEN '213'
                                        WHEN 'SPECIAL EDUCATION' THEN '214'
                                        WHEN 'SPECIALTY-CONTRACTOR' THEN '215'
                                        WHEN 'SPORTS MEDICINE' THEN '216'
                                        WHEN 'STRUCTURAL' THEN '217'
                                        WHEN 'SUPERINTENDENT' THEN '218'
                                        WHEN 'SURETY (ERRORS & OMISSIONS)' THEN '219'
                                        WHEN 'SURGERY' THEN '220'
                                        WHEN 'SURGICAL' THEN '221'
                                        WHEN 'SURPLUS LINES' THEN '222'
                                        WHEN 'TAXIDERMY' THEN '223'
                                        WHEN 'TECHNICIAN' THEN '224'
                                        WHEN 'TELEVISION' THEN '225'
                                        WHEN 'TITLE AGENT/ABSTRACTOR' THEN '226'
                                        WHEN 'TITLE INSURANCE' THEN '227'
                                        WHEN 'TOXICOLOGY' THEN '228'
                                        WHEN 'TRANSPLANTS' THEN '229'
                                        WHEN 'TRANSPORTATION' THEN '230'
                                        WHEN 'TRAVEL' THEN '231'
                                        WHEN 'UNKNOWN/GENERAL' THEN '232'
                                        WHEN 'UROLOGY' THEN '233'
                                        WHEN 'USED' THEN '234'
                                        WHEN 'WASTE WATER TREATMENT' THEN '235'
                                        WHEN 'WATER RESOURCES' THEN '236'
                                        WHEN 'WATER WELLS' THEN '237'
                                        WHEN 'WETLAND DELINEATOR' THEN '238'
                                        WHEN 'WINE' THEN '239'
                                        WHEN 'WINE, LIQUOR' THEN '240'
                                        WHEN 'WOMENS HEALTH' THEN '241'
                                        ELSE '' END AS specialtytitlecode,
    REPLACE(a.addressline1 + a.city + a.state + a.zip, ' ', '') AS hardkey,
    CAST(LEFT(RIGHT('000' + CAST(CAST(a.longitude AS INT) AS VARCHAR),9),3) + '.' + RIGHT(RIGHT('000' + CAST(CAST(a.longitude AS INT) AS VARCHAR),9),6) AS FLOAT) AS flongitude,
    CAST(LEFT(RIGHT('000' + CAST(CAST(a.latitude AS INT) AS VARCHAR),9),3) + '.' + RIGHT(RIGHT('000' + CAST(CAST(a.latitude AS INT) AS VARCHAR),9),6) AS FLOAT) AS flatitude,
    CASE WHEN a.state IS NULL OR b.ccounty IS NULL 
         THEN '' 
         ELSE a.state + b.ccounty END AS statecountyname,
    CASE WHEN a.state IS NULL OR LTRIM(RTRIM(UPPER(a.city))) IS NULL 
         THEN '' 
         ELSE a.state + LTRIM(RTRIM(UPPER(a.city))) END AS statecity,
    CASE WHEN a.state IS NULL OR LTRIM(RTRIM(UPPER(a.city))) IS NULL OR LTRIM(RTRIM(a.zip)) IS NULL  
         THEN '' 
         ELSE a.state + LTRIM(RTRIM(UPPER(a.city)))+ LTRIM(RTRIM(a.zip)) END AS statecityzip,
    CASE WHEN a.state IS NULL OR LTRIM(RTRIM(a.censuscountycode)) IS NULL 
         THEN '' 
         ELSE a.state + LTRIM(RTRIM(a.censuscountycode)) END AS statecountycode,
    CASE WHEN a.state IS NULL OR LTRIM(RTRIM(UPPER(a.city))) IS NULL OR LTRIM(RTRIM(a.scf)) IS NULL 
         THEN '' 
         ELSE a.state + LTRIM(RTRIM(UPPER(a.city)))+ LTRIM(RTRIM(a.scf)) END AS statecityscf,
    CASE WHEN UPPER(LTRIM(RTRIM(a.state))+LTRIM(RTRIM(a.county_code))) = UPPER(LTRIM(RTRIM(c.cCode))) 
         THEN c.cDescription 
         ELSE '' END AS county_code_description,
    CASE WHEN (a.area_code + a.phone_number = d.cPhone and a.area_code + a.phone_number <>'') THEN 'Y'
		 WHEN a.state_donotcall_flag ='Y' THEN 'Y' 
           ELSE 'N' END AS donotcallflag ,
     a.individual_mc_wb as individual_mc_wb,
     a.company_mc_wb as company_mc_wb
FROM {tablename1} a
LEFT JOIN
(
    SELECT LTRIM(RTRIM(UPPER(cstatecode))) AS cstatecode , LTRIM(RTRIM(UPPER(ccountycode))) AS ccountycode, LTRIM(RTRIM(UPPER(ccounty))) AS ccounty
    FROM sql_tblstate WHERE databaseid=0 
    GROUP BY cstatecode, ccountycode, ccounty
) b
ON a.state = b.cstatecode AND a.censuscountycode = b.ccountycode
LEFT JOIN
(
    SELECT LTRIM(RTRIM(UPPER(cDescription))) AS cdescription, LTRIM(RTRIM(UPPER(cCode))) as ccode
    FROM countystate_decode GROUP BY cdescription,ccode
) c
ON UPPER(LTRIM(RTRIM(a.state))+ LTRIM(RTRIM(a.county_code))) = UPPER(LTRIM(RTRIM(c.ccode)))
LEFT JOIN 
(
    SELECT cphone FROM exclude_donotcallflag GROUP BY cphone
) d
ON a.area_code + a.phone_number = d.cphone AND a.area_code + a.phone_number <> '';


DROP TABLE IF EXISTS {tablename1};
ALTER TABLE temp_{tablename1} RENAME TO {tablename1};