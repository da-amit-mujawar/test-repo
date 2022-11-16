--create table for loading to redshift
drop table if exists {table_consumer_email_raw};

create table {table_consumer_email_raw}
(   
acquisition_date	varchar(	6	) null
,	vendorid2	varchar(	2	) null
,	usage_indicator	varchar(	1	) null
,	firstname	varchar(	15	) null
,	middleinitial	varchar(	1	) null
,	lastname	varchar(	20	) null
,	gender	varchar(	1	) null
,	housenumber	varchar(	10	) null
,	streetpredirectional	varchar(	2	) null
,	streetname	varchar(	28	) null
,	streetsuffix	varchar(	4	) null
,	streetpostdirectional	varchar(	2	) null
,	unittype	varchar(	4	) null
,	unitnumber	varchar(	8	) null
,	city	varchar(	28	) null
,	state	varchar(	2	) null
,	zipcode	varchar(	6	) null
,	zipfour	varchar(	4	) null
,	move_ind	varchar(	1	) null
,	emailaddress	varchar(	80	) null
,	responderdate	varchar(	6	) null
,	suppression_type	varchar(	1	) null
,	familyid	varchar(	12	) null
,	individualid	varchar(	12	) null
,	match_ind	varchar(	1	) null
,	mailconfidence	varchar(	2	) null
,	recordtype	varchar(	1	) null
,	alsincome	varchar(	1	) null
,	alsagecode	varchar(	1	) null
,	alslengthofresidence	varchar(	2	) null
,	alspurchasingpowerincome	varchar(	1	) null
,	alshomevalue	varchar(	1	) null
,	alswealthfinder	varchar(	1	) null
,	psfducode	varchar(	1	) null
,	p10ducode	varchar(	1	) null
,	pownrocccode	varchar(	1	) null
,	ip_address	varchar(	15	) null
,	optin_date	varchar(	8	) null
,	url	varchar(	60	) null
,	statecode	varchar(	2	) null
,	censuscountycode	varchar(	3	) null
,	censustract	varchar(	6	) null
,	censusblockgroup	varchar(	1	) null
,	matchcode	varchar(	1	) null
,	editedaddress	varchar(	40	) null
,	emaildatabase_extent	varchar(	2	) null
,	oldmasterindicator	varchar(	1	) null
,	prioritysourcecode	varchar(	2	) null
,	roadrunner_flag	varchar(	1	) null
,	source_counter	varchar(	2	) null
,	source_code_indicator1	varchar(	10	) null
,	source_code_indicator2	varchar(	10	) null
,	source_code_indicator3	varchar(	10	) null
,	latitude	varchar(	9	) null
,	longitude	varchar(	9	) null
,	contactid	varchar(	12	) null
,	gst_sourcecode_indicator	varchar(	10	) null
,	gst_source_counter	varchar(	2	) null
,	lemsmatchcode	varchar(	18	) null
,	reject_reason	varchar(	3	) null
,	dma_code	varchar(	3	) null
,	bvt_email_status	varchar(	1	) null
,	bvt_refresh_date	varchar(	8	) null
,	ipst_validity_score	varchar(	1	) null
,	ipst_status_code	varchar(	1	) null
,	ipst_refresh_date	varchar(	8	) null
,	category	varchar(	36	) null
,	email_clickthru_date	varchar(	8	) null
,	email_open_date	varchar(	8	) null
,	domain	varchar(	80	) null
,	top_level_domain	varchar(	6	) null
,	dob_individual	varchar(	8	) null
,	home_owner	varchar(	1	) null
,	best_date	varchar(	8	) null
,	emaildb_flag	varchar(	1	) null
,	countrycode	varchar(	2	) null
,   emailaddress_md5upper varchar(32) null
,   emailaddress_md5lower varchar(32) null
,	seq bigint
	) ;


drop table if exists {table_business_email_raw};

create table {table_business_email_raw} (
	acquisition_date	varchar(	6	) null
,	vendorid2	varchar(	2	) null
,	usage_indicator	varchar(	1	) null
,	firstname	varchar(	15	) null
,	middleinitial	varchar(	1	) null
,	lastname	varchar(	20	) null
,	gender	varchar(	1	) null
,	housenumber	varchar(	10	) null
,	streetpredirectional	varchar(	2	) null
,	streetname	varchar(	28	) null
,	streetsuffix	varchar(	4	) null
,	streetpostdirectional	varchar(	2	) null
,	unittype	varchar(	4	) null
,	unitnumber	varchar(	8	) null
,	city	varchar(	28	) null
,	state	varchar(	2	) null
,	zipcode	varchar(	6	) null
,	zipfour	varchar(	4	) null
,	move_ind	varchar(	1	) null
,	emailaddress	varchar(	80	) null
,	responderdate	varchar(	6	) null
,	suppression_type	varchar(	1	) null
,	familyid	varchar(	12	) null
,	individualid	varchar(	12	) null
,	match_ind	varchar(	1	) null
,	mailconfidence	varchar(	2	) null
,	recordtype	varchar(	1	) null
,	alsincome	varchar(	1	) null
,	alsagecode	varchar(	1	) null
,	alslengthofresidence	varchar(	2	) null
,	alspurchasingpowerincome	varchar(	1	) null
,	alshomevalue	varchar(	1	) null
,	alswealthfinder	varchar(	1	) null
,	psfducode	varchar(	1	) null
,	p10ducode	varchar(	1	) null
,	pownrocccode	varchar(	1	) null
,	ip_address	varchar(	15	) null
,	optin_date	varchar(	8	) null
,	url	varchar(	60	) null
,	statecode	varchar(	2	) null
,	censuscountycode	varchar(	3	) null
,	censustract	varchar(	6	) null
,	censusblockgroup	varchar(	1	) null
,	matchcode	varchar(	1	) null
,	editedaddress	varchar(	40	) null
,	emaildatabase_extent	varchar(	2	) null
,	oldmasterindicator	varchar(	1	) null
,	prioritysourcecode	varchar(	2	) null
,	roadrunner_flag	varchar(	1	) null
,	source_counter	varchar(	2	) null
,	source_code_indicator1	varchar(	10	) null
,	source_code_indicator2	varchar(	10	) null
,	source_code_indicator3	varchar(	10	) null
,	latitude	varchar(	9	) null
,	longitude	varchar(	9	) null
,	contactid	varchar(	12	) null
,	gst_sourcecode_indicator	varchar(	10	) null
,	gst_source_counter	varchar(	2	) null
,	lemsmatchcode	varchar(	18	) null
,	reject_reason	varchar(	3	) null
,	dma_code	varchar(	3	) null
,	bvt_email_status	varchar(	1	) null
,	bvt_refresh_date	varchar(	8	) null
,	ipst_validity_score	varchar(	1	) null
,	ipst_status_code	varchar(	1	) null
,	ipst_refresh_date	varchar(	8	) null
,	category	varchar(	36	) null
,	email_clickthru_date	varchar(	8	) null
,	email_open_date	varchar(	8	) null
,	domain	varchar(	80	) null
,	top_level_domain	varchar(	6	) null
,	dob_individual	varchar(	8	) null
,	home_owner	varchar(	1	) null
,	best_date	varchar(	8	) null
,	emaildb_flag	varchar(	1	) null
,	countrycode	varchar(	2	) null
,	abinumber	varchar(	9	) null
,	literaltitle	varchar(	8	) null
,	titlecode	varchar(	2	) null
,	proftitle	varchar(	3	) null
,	executivesourcecode	varchar(	2	) null
,	database_flag	varchar(	1	) null
,	companyname	varchar(	30	) null
,	areacode	varchar(	3	) null
,	phonenumber	varchar(	7	) null
,	siccode	varchar(	6	) null
,	business_phonenumber_flag	varchar(	1	) null
,	sec_business_areacode	varchar(	3	) null
,	sec_business_ponenumber	varchar(	7	) null
,	sec_business_phonenumber_flag	varchar(	1	) null
,    emailaddress_md5upper varchar(32) null
,    emailaddress_md5lower varchar(32) null
,	seq bigint
);


drop table if exists {table_business_sic};

create table {table_business_sic} (
 abinumber	varchar(9)  null,
s_actioncode1	varchar(1)  null,
s_actioncode2	varchar(1)  null,
s_adsizecode	varchar(1)  null,
s_attorneycuisinecode	varchar(1)  null,
s_bankassetcode	varchar(1)  null,
s_booknumber	varchar(5)  null,
s_dateindatabase	varchar(6)  null,
s_franchisecode1	varchar(1)  null,
s_franchisecode1_extended	varchar(3)  null,
s_franchisecode2	varchar(1)  null,
s_franchisecode2_extended	varchar(3)  null,
s_franchisecode3	varchar(1)  null,
s_franchisecode3_extended	varchar(3)  null,
s_franchisecode4	varchar(1)  null,
s_franchisecode4_extended	varchar(3)  null,
s_franchisecode5	varchar(1)  null,
s_franchisecode5_extended	varchar(3)  null,
s_franchisecode6	varchar(1)  null,
s_franchisecode6_extended	varchar(3)  null,
s_industryspecificfirstbyte	varchar(1)  null,
s_naics6	varchar(6)  null,
s_newadddate	varchar(6)  null,
s_primarysicdesignator	varchar(1)  null,
s_professionalsicflag	varchar(1)  null,
s_siccode_altbase	varchar(8)  null,
s_siccode	varchar(6)  null,
s_sicyearfirstappeared	varchar(4)  null,
s_truefranchiseflag	varchar(1)  null,
s_updatedate	varchar(6)  null,
s_yellowpagecode	varchar(5)  null,
siccount	varchar(6)  null,
s_pnaics2	varchar(2)  null,
s_naics2	varchar(2)  null,
s_pnaics4	varchar(4)  null,
s_naics4	varchar(4)  null,
s_bookpublicationdate	varchar(6)  null,
s_frenchflag	varchar(1)  null,
seq bigint
);

drop table if exists {table_business_exec_raw};

create table {table_business_exec_raw} (
abinumber	varchar(	9	) null,
executivedetailflag	varchar(	1	) null,
e_femaleexecflag	varchar(	1	) null,
e_firstname	varchar(	11	) null,
e_gender	varchar(	1	) null,
e_highincomeexecflag	varchar(	1	) null,
e_lastname	varchar(	14	) null,
e_proftitle	varchar(	6	) null,
e_executivesourcecode	varchar(	2	) null,
e_salutationcode	varchar(	3	) null,
e_titlecode	varchar(	1	) null,
e_literaltitle	varchar(	8	) null,
e_typecode	varchar(	1	) null,
e_vendorethnicgroup	varchar(	3	) null,
e_vendorethnicity	varchar(	2	) null,
e_vendorlanguage	varchar(	3	) null,
e_vendorreligion	varchar(	1	) null,
e_countryoforigin	varchar(	3	) null,
e_emailflag	varchar(	1	) null,
e_emailaddress	varchar(	60	) null,
e_vendorid2	varchar(	2	) null,
email_suppress	varchar(	1	) null,
e_bvt_email_status	varchar(	1	) null,
e_areacode_v1	varchar(	3	) null,
e_phonenumber_v1	varchar(	7	) null,
amiexec_abinumber	varchar(	9	) null,
amiexec_age	varchar(	2	) null,
amiexec_amicreditcardpresence	varchar(	1	) null,
amiexec_execgender	varchar(	1	) null,
amiexec_homeaddress	varchar(	30	) null,
amiexec_homecity	varchar(	18	) null,
amiexec_homestate	varchar(	2	) null,
amiexec_homezipcode	varchar(	5	) null,
amiexec_homezipfour	varchar(	4	) null,
amiexec_homecountycode	varchar(	3	) null,
amiexec_homeowner	varchar(	1	) null,
amiexec_incomecode	varchar(	1	) null,
amiexec_locationtype	varchar(	1	) null,
amiexec_maritalstatus	varchar(	1	) null,
amiexec_homeareacode	varchar(	3	) null,
amiexec_homephonenumber	varchar(	7	) null,
amiexec_nonsolicitationflag	varchar(	1	) null,
e_contactid	varchar(	12	) null,
e_execage	varchar(	2	) null,
e_creditcardpresence	varchar(	1	) null,
e_execgender	varchar(	1	) null,
e_homeaddress	varchar(	30	) null,
e_homecity	varchar(	30	) null,
e_homestate	varchar(	2	) null,
e_homezipcode	varchar(	5	) null,
e_homezip4	varchar(	4	) null,
e_homecountycode	varchar(	3	) null,
e_homeowner	varchar(	1	) null,
e_execincome	varchar(	1	) null,
e_homelocationtype	varchar(	1	) null,
e_maritalstatus	varchar(	1	) null,
e_homeareacode	varchar(	3	) null,
e_homephonenumber	varchar(	7	) null,
e_donotcallflag	varchar(	1	) null,
e_homelatitude	varchar(	9	) null,
e_homelongitude	varchar(	9	) null,
e_homematchcode	varchar(	1	) null,
e_individualid	varchar(	12	) null,
e_familyid	varchar(	12	) null,
e_sesi	varchar(	2	) null,
e_homevaluecode	varchar(	1	) null,
e_emailaddress_md5upper varchar(32) null,
e_emailaddress_md5lower varchar(32) null,
e_departmentcode varchar( 2 ) null,
e_functionalareacode varchar( 4 ) null,
e_levelcode varchar( 1 ) null,
e_rolecode varchar( 4 ) null,
seq bigint
);

drop table if exists {table_business_main};

create table {table_business_main} (
abinumber	varchar(9)	  null,
sitenumber	varchar(9)	  null,
locationname	varchar(30)	  null,
businessname	varchar(30)	  null,
address	varchar(70)	  null,
city	varchar(50)	  null,
state	varchar(2)	  null,
zipcode	varchar(5)	  null,
zipfour	varchar(4)	  null,
scf	varchar(3)	  null,
carrierroutecode	varchar(4)	  null,
countycode	varchar(3)	  null,
csacode	varchar(3)	  null,
cbsacode	varchar(5)	  null,
cbsalevel	varchar(1)	  null,
censusblock	varchar(1)	  null,
censustract	varchar(6)	  null,
areacode	varchar(3)	  null,
phonenumber	varchar(7)	  null,
phoneprefix	varchar(3)	  null,
phoneexchange	varchar(4)	  null,
faxareacode	varchar(3)	  null,
faxphonenumber	varchar(7)	  null,
amifaxareacode	varchar(3)	  null,
amifaxphonenumber	varchar(7)	  null,
attorneyfaxareacode	varchar(3)	  null,
attorneyfaxphonenumber	varchar(7)	  null,
tollfreeareacode	varchar(3)	  null,
tollfreephonenumber	varchar(7)	  null,
bigbusinesssegmentationcode	varchar(1)	  null,
businessstatuscode	varchar(1)	  null,
businessstatuscode_year1	varchar(1)	  null,
businessstatuscode_year2	varchar(1)	  null,
callstatuscode	varchar(1)	  null,
companyholdingstatus	varchar(1)	  null,
companyholdingstatus_year1	varchar(1)	  null,
companyholdingstatus_year2	varchar(1)	  null,
modeledemployeesizeflag	varchar(1)	  null,
actualemployees	varchar(6)	  null,
employeesizecode	varchar(1)	  null,
numberemployees	varchar(5)	  null,
numberemployees_year1	varchar(5)	  null,
numberemployees_year2	varchar(5)	  null,
officesizecode	varchar(1)	  null,
outputvolume	varchar(9)	  null,
outputvolumecode	varchar(1)	  null,
outputvolumetypecode	varchar(1)	  null,
ownorlease	varchar(1)	  null,
corporatedetailflag	varchar(1)	  null,
corporateemployees	varchar(6)	  null,
corporateemployeesize	varchar(1)	  null,
corporatenumberemployees_year1	varchar(6)	  null,
corporatenumberemployees_year2	varchar(6)	  null,
corporateoutputvolume	varchar(9)	  null,
corporateoutputvolume_year1	varchar(9)	  null,
corporateoutputvolume_year2	varchar(9)	  null,
corporatesalesvolume	varchar(1)	  null,
cottagecode	varchar(1)	  null,
creditalphascore	varchar(2)	  null,
creditcardsaccepted	varchar(15)	  null,
creditlimit	varchar(6)	  null,
creditnumericscore	varchar(3)	  null,
creditratingcode	varchar(1)	  null,
databasesegmentationcode	varchar(1)	  null,
deliverypointbarcode	varchar(3)	  null,
fortuneranking	varchar(4)	  null,
governmentflag	varchar(1)	  null,
growingbusinessflag	varchar(1)	  null,
actioncode1	varchar(1)	  null,
actioncode2	varchar(1)	  null,
exp_acc	varchar(4)	  null,
exp_adv	varchar(4)	  null,
exp_clc	varchar(4)	  null,
exp_cmp	varchar(4)	  null,
exp_com	varchar(4)	  null,
exp_ins	varchar(4)	  null,
exp_leg	varchar(4)	  null,
exp_ofc	varchar(4)	  null,
exp_pac	varchar(4)	  null,
exp_pay	varchar(4)	  null,
exp_prn	varchar(4)	  null,
exp_pro	varchar(4)	  null,
exp_rnt	varchar(4)	  null,
exp_trn	varchar(4)	  null,
exp_utl	varchar(4)	  null,
exptype	varchar(1)	  null,
hightechbusinessflag	varchar(1)	  null,
identificationcode	varchar(1)	  null,
importexportflag	varchar(1)	  null,
lastdate	varchar(6)	  null,
linkagedown	varchar(1)	  null,
linkageup	varchar(1)	  null,
foreignparentflag	varchar(1)	  null,
parent	varchar(9)	  null,
subsidiary	varchar(9)	  null,
payatpump	varchar(1)	  null,
populationcode	varchar(1)	  null,
propertymgtabinumber	varchar(9)	  null,
aclflag	varchar(1)	  null,
acl_aclcallstatus	varchar(1)	  null,
acl_aclrecordtype	varchar(1)	  null,
acl_attendance	varchar(5)	  null,
acl_attendancecode	varchar(1)	  null,
acl_attendancelastverificationdate	varchar(8)	  null,
acl_buyercode1	varchar(2)	  null,
acl_buyercode2	varchar(2)	  null,
acl_buyercode3	varchar(2)	  null,
acl_buyercode4	varchar(2)	  null,
acl_buyercode5	varchar(2)	  null,
acl_buyercode6	varchar(2)	  null,
acl_buyercode7	varchar(2)	  null,
acl_buyercode8	varchar(2)	  null,
acl_buyercode9	varchar(2)	  null,
acl_buyercode10	varchar(2)	  null,
acl_buyercode11	varchar(2)	  null,
acl_buyercode12	varchar(2)	  null,
acl_buyercode13	varchar(2)	  null,
acl_buyercode14	varchar(2)	  null,
acl_buyercode15	varchar(2)	  null,
acl_buyercode16	varchar(2)	  null,
acl_buyercode17	varchar(2)	  null,
acl_buyercode18	varchar(2)	  null,
acl_buyercode19	varchar(2)	  null,
acl_buyercode20	varchar(2)	  null,
acl_buyercode21	varchar(2)	  null,
acl_buyercode22	varchar(2)	  null,
acl_buyercode23	varchar(2)	  null,
acl_buyercode24	varchar(2)	  null,
acl_buyercode25	varchar(2)	  null,
acl_buyercode26	varchar(2)	  null,
acl_buyercode27	varchar(2)	  null,
acl_buyercode28	varchar(2)	  null,
acl_buyercode29	varchar(2)	  null,
acl_buyercode30	varchar(2)	  null,
acl_buyercode31	varchar(2)	  null,
acl_buyercode32	varchar(2)	  null,
acl_buyercode33	varchar(2)	  null,
acl_buyercode34	varchar(2)	  null,
acl_buyercode35	varchar(2)	  null,
acl_churchsize	varchar(5)	  null,
acl_churchsizecode	varchar(1)	  null,
acl_churchsizelastverificationdate	varchar(8)	  null,
acl_dateconstructionplanned	varchar(8)	  null,
acl_dateindatabase	varchar(8)	  null,
nbrcnewadddate	varchar(8)	  null,
acl_denominationcode	varchar(4)	  null,
acl_denominationlastverificationdate	varchar(8)	  null,
acl_groupcode	varchar(2)	  null,
acl_lastverificationdate	varchar(8)	  null,
acl_membershipsourceflag	varchar(1)	  null,
acl_multiplecongregationscode	varchar(1)	  null,
acl_multiplecongregationslastverificationdate	varchar(8)	  null,
acl_newconstructionbudget	varchar(6)	  null,
acl_newconstructionlastverificationdate	varchar(8)	  null,
acl_newconstructionplanned	varchar(1)	  null,
acl_racelastverificationdate	varchar(8)	  null,
acl_raceofcongregation1	varchar(1)	  null,
acl_raceofcongregation2	varchar(1)	  null,
acl_raceofcongregation3	varchar(1)	  null,
acl_raceofcongregation4	varchar(1)	  null,
acl_raceofcongregation5	varchar(1)	  null,
acl_raceofcongregation6	varchar(1)	  null,
acl_remodelingbudget	varchar(50)	  null,
acl_remodelinglastverificationdate	varchar(8)	  null,
acl_remodelingplanned	varchar(1)	  null,
acl_schoolsize	varchar(5)	  null,
acl_schoolsizecode	varchar(1)	  null,
acl_schoolsizelastverificationdate	varchar(8)	  null,
acl_updatedate	varchar(8)	  null,
aclexeccount	varchar(6)	  null,
acl_actioncode	varchar(1)	  null,
g_growthcode	varchar(1)	  null,
g_growthtrend	varchar(1)	  null,
amiflag	varchar(1)	  null,
ami_associationcode	varchar(3)	  null,
ami_boardcertifiedindicator	varchar(1)	  null,
ami_dateofbirth	varchar(6)	  null,
ami_deanumber	varchar(9)	  null,
ami_fellowshipcode	varchar(7)	  null,
ami_fellowshipyear	varchar(4)	  null,
ami_hospitalnumber	varchar(7)	  null,
ami_languagecode1	varchar(3)	  null,
ami_languagecode2	varchar(3)	  null,
ami_languagecode3	varchar(3)	  null,
ami_languagecode4	varchar(3)	  null,
ami_languagecode5	varchar(3)	  null,
ami_medicalschoolcode	varchar(5)	  null,
ami_networkcode	varchar(3)	  null,
ami_npi	varchar(10)	  null,
ami_numberofchairs	varchar(5)	  null,
ami_numberofnurses	varchar(5)	  null,
ami_nursepractitioners	varchar(5)	  null,
ami_officemanagerfirstname	varchar(11)	  null,
ami_officemanagerlastname	varchar(14)	  null,
ami_prescriptionsperwk	varchar(1)	  null,
ami_primaryspecialty	varchar(3)	  null,
ami_professionaldegreecode	varchar(1)	  null,
ami_residencycode	varchar(7)	  null,
ami_residencyyear	varchar(4)	  null,
ami_secondaryspecialty	varchar(3)	  null,
ami_sizegrouppractice	varchar(3)	  null,
ami_totalpatientsseenweekly	varchar(4)	  null,
ami_typeofpractice	varchar(1)	  null,
ami_upin	varchar(6)	  null,
ami_yearofgraduation	varchar(4)	  null,
amiact_activitycode_0	varchar(2)	  null,
amiact_activitycode_1	varchar(2)	  null,
amiact_activitycode_2	varchar(2)	  null,
amiact_activitycode_3	varchar(2)	  null,
amiact_activitycode_4	varchar(2)	  null,
amiact_activitycode_5	varchar(2)	  null,
amiact_activitycode_6	varchar(2)	  null,
ami_currentlinkagenumber	varchar(9)	  null,
amilic_stateoflicense	varchar(2)	  null,
ami_addresstype	varchar(1)	  null,
amidis_diseasecode	varchar(2)	  null,
amilic_licenseboardtype	varchar(3)	  null,
amilic_licensenumber	varchar(50)	  null,
amilic_expirationdate	varchar(8)	  null,
addresstypeindicator	varchar(2)	  null,
a_apartmentsuite	varchar(20)	  null,
a_housenumber	varchar(15)	  null,
a_streetname	varchar(28)	  null,
a_streetpostdirectional	varchar(2)	  null,
a_streetpredirectional	varchar(2)	  null,
a_streetsuffix	varchar(50)	  null,
m_address_mailing	varchar(30)	  null,
m_addressdetailtype_mailing	varchar(1)	  null,
m_addresstypeindicator_mailing	varchar(2)	  null,
m_carrierroutecode_mailing	varchar(4)	  null,
m_city_mailing	varchar(16)	  null,
m_deliverypointbarcode_mailing	varchar(3)	  null,
m_mailconfidence_mailing_mailing	varchar(1)	  null,
m_mailscore_mailing_mailing	varchar(2)	  null,
m_state_mailing	varchar(2)	  null,
m_zipcode_mailing	varchar(5)	  null,
m_zipfour_mailing	varchar(4)	  null,
m_address_landmark	varchar(30)	  null,
m_addressdetailtype_landmark	varchar(1)	  null,
m_addresstypeindicator_landmark	varchar(2)	  null,
m_city_landmark	varchar(16)	  null,
m_state_landmark	varchar(2)	  null,
m_zipcode_landmark	varchar(5)	  null,
m_mailscore_mailing_landmark	varchar(2)	  null,
m_mailconfidence_mailing_landmark	varchar(1)	  null,
mailconfidence	varchar(1)	  null,
mailingdetailflag	varchar(1)	  null,
mailscore	varchar(2)	  null,
multitenantcode	varchar(1)	  null,
multitenantnumber	varchar(7)	  null,
buildingnumber	varchar(7)	  null,
int_latitude	varchar(9)	  null,
int_longitude	varchar(9)	  null,
int_matchcode	varchar(1)	  null,
latitude	varchar(9)	  null,
longitude	varchar(9)	  null,
matchtype	varchar(1)	  null,
latitudedeg	varchar(3)	  null,
latitudehrs	varchar(2)	  null,
latitudemin	varchar(2)	  null,
latitudesec	varchar(2)	  null,
latitude_degmin	varchar(5)	  null,
longitudedeg	varchar(3)	  null,
longitudehrs	varchar(2)	  null,
longitudemin	varchar(2)	  null,
longitudesec	varchar(2)	  null,
longitude_degmin	varchar(5)	  null,
namestandardizationflag	varchar(1)	  null,
nid	varchar(6)	  null,
filingflag	varchar(1)	  null,
p_action_type	varchar(2)	  null,
p_amountorliability	varchar(9)	  null,
p_assets_available_for_distribution	varchar(1)	  null,
p_case_number	varchar(50)	  null,
p_case_number_1	varchar(50)	  null,
p_case_number_2	varchar(50)	  null,
p_case_number_3	varchar(50)	  null,
p_case_number_4	varchar(50)	  null,
p_case_number_5	varchar(50)	  null,
p_case_number_6	varchar(50)	  null,
p_case_number_7	varchar(50)	  null,
p_case_number_8	varchar(50)	  null,
p_countyoffiling	varchar(50)	  null,
p_filing_date	varchar(8)	  null,
p_filing_type	varchar(2)	  null,
p_release_date	varchar(8)	  null,
p_sourcecode	varchar(1)	  null,
p_countyoffiling_0_altdictionary	varchar(2)	  null,
p_bankruptcy_dismissal_flag	varchar(1)	  null,
p_date_of_upload	varchar(8)	  null,
p_defendant_name	varchar(32)	  null,
p_defendant_street_address	varchar(50)	  null,
p_defendant_city	varchar(50)	  null,
p_defendant_state	varchar(2)	  null,
p_defendant_zipcode	varchar(10)	  null,
p_department_cvsc_or_lien	varchar(2)	  null,
p_entity_id	varchar(2)	  null,
p_assets	varchar(9)	  null,
p_association_code	varchar(1)	  null,
p_attorney	varchar(50)	  null,
p_attorney_address	varchar(50)	  null,
p_attorney_city	varchar(50)	  null,
p_attorney_or_phone	varchar(10)	  null,
p_attorney_state	varchar(2)	  null,
p_attorney_zipcode	varchar(10)	  null,
p_book	varchar(6)	  null,
p_county_of_residency	varchar(5)	  null,
p_locationname	varchar(32)	  null,
p_match_date	varchar(8)	  null,
p_match_type	varchar(2)	  null,
p_original_case	varchar(17)	  null,
p_other_case_number	varchar(13)	  null,
p_page	varchar(8)	  null,
p_plaintiff_or_law_firm	varchar(32)	  null,
p_rms_id	varchar(10)	  null,
p_schedule_341_date	varchar(8)	  null,
p_schedule_341_time	varchar(4)	  null,
p_judges_initials	varchar(3)	  null,
p_his_court_id	varchar(7)	  null,
servicetype	varchar(1)	  null,
smmedbusseg	varchar(1)	  null,
squarefootage	varchar(10)	  null,
stockexchangecode	varchar(1)	  null,
stocktickersymbol	varchar(50)	  null,
sundayopen	varchar(4)	  null,
sundayclose	varchar(4)	  null,
mondayopen	varchar(4)	  null,
mondayclose	varchar(4)	  null,
tuesdayopen	varchar(4)	  null,
tuesdayclose	varchar(4)	  null,
wednesdayopen	varchar(4)	  null,
wednesdayclose	varchar(4)	  null,
thursdayopen	varchar(4)	  null,
thursdayclose	varchar(4)	  null,
fridayopen	varchar(4)	  null,
fridayclose	varchar(4)	  null,
saturdayopen	varchar(4)	  null,
saturdayclose	varchar(4)	  null,
truenewadd	varchar(4)	  null,
currenttransactioncode	varchar(1)	  null,
primarywebaddressflag	varchar(1)	  null,
activewebaddressflag	varchar(1)	  null,
webaddress	varchar(40)	  null,
websiteflag	varchar(1)	  null,
utilitysource	varchar(1)	  null,
wealthcode	varchar(1)	  null,
whitecollar	varchar(3)	  null,
teleresearchupdatedate	varchar(6)	  null,
yearcode	varchar(1)	  null,
yearestablished	varchar(4)	  null,
yearfirstappeared	varchar(4)	  null,
recordid	varchar(9)	  null,
expressupdate	varchar(8)	  null,
dmacode	varchar(3)	  null,
populationdensity_extended	varchar(10)	  null,
fipscode	varchar(5)	  null,
numberpcs	varchar(5)	  null,
pccode	varchar(2)	  null,
multiexec	varchar(1)	  null,
build_date	varchar(8)	  null,
fulfillmentflag	varchar(1)	  null,
grade	varchar(1)	  null,
compdate	varchar(6)	  null,
lasttype	varchar(1)	  null,
phonenumbertype	varchar(1)	  null,
ypdate	varchar(6)	  null,
latitude_v	varchar(9)	  null,
longitude_v	varchar(9)	  null,
matchcode_v	varchar(1)	  null,
amilic_licensereinstatedate	varchar(8)	  null,
infoscore	varchar(3)	  null,
locationscore	varchar(3)	  null,
industrygrowthscore	varchar(3)	  null,
scalescore	varchar(3)	  null,
contactcontinuityscore	varchar(3)	  null,
completenessscore	varchar(3)	  null,
stabilityscore	varchar(3)	  null,
reachabilityscore	varchar(3)	  null,
infograde	varchar(2)	  null,
locationgrade	varchar(2)	  null,
industrygrowthgrade	varchar(2)	  null,
scalegrade	varchar(2)	  null,
contactcontinuitygrade	varchar(2)	  null,
completenessgrade	varchar(2)	  null,
stabilitygrade	varchar(2)	  null,
reachabilitygrade	varchar(2)	  null,
countrycode	varchar(2)	  null,
auditorfirmcode	varchar(1)	  null,
cdname	varchar(150)	  null,
comm_name	varchar(50)	  null,
csd	varchar(3)	  null,
csdname	varchar(150)	  null,
fiscalyearend	varchar(1)	  null,
m_mailaddresscantzipflag	varchar(1)	  null,
m_mailaddressstandardizationflag	varchar(4)	  null,
postalcode	varchar(10)	  null,
preferredcorrespondence	varchar(50)	  null,
sacname	varchar(50)	  null,
sohoflag	varchar(1)	  null,
suitenumber	varchar(8)	  null,
address_nosuite	varchar(30)	  null,
m_zipfour_landmark	varchar(5)	  null,
m_postalcode_mailing	varchar(50)	  null,
m_postalcode_landmark	varchar(50)	  null,
m_address_nosuite_mailing	varchar(30)	  null,
m_suitenumber_mailing	varchar(50)	  null,
m_suitenumber_landmark	varchar(8)	  null,
squarefootage8	varchar(1)	  null,
ccaflag	varchar(1)	  null,
companydescription	varchar(5000)	  null,
townshipcode	varchar(5000)	  null,
filler_delete_column varchar(2500) null,
seq bigint
);

drop table if exists {table_business_occupations};

create table {table_business_occupations} (
occupation_id	varchar (	12	) null,
first_name	varchar (	15	) null,
middle_name	varchar (	15	) null,
last_name	varchar (	20	) null,
generational_suffix	varchar (	4	) null,
address	varchar (	40	) null,
house_number	varchar (	10	) null,
street_pre_directional	varchar (	2	) null,
street_name	varchar (	28	) null,
street_suffix	varchar (	4	) null,
street_post_directional	varchar (	2	) null,
unit_type	varchar (	4	) null,
unit_number	varchar (	8	) null,
city	varchar (	28	) null,
state	varchar (	2	) null,
zip_code	varchar (	5	) null,
zip_four	varchar (	4	) null,
delivery_point_barcode	varchar (	3	) null,
high_rise	varchar (	2	) null,
carrier_route_code	varchar (	4	) null,
census_state_code	varchar (	2	) null,
census_county_code	varchar (	3	) null,
county_code_desc	varchar (	5	) null,
census_tract	varchar (	6	) null,
census_block_group	varchar (	1	) null,
match_code	varchar (	1	) null,
latitude	varchar (	9	) null,
longitude	varchar (	9	) null,
mail_score	varchar (	2	) null,
residential_business_indicator	varchar (	1	) null,
employer_name	varchar (	30	) null,
family_id	varchar (	12	) null,
individual_id	varchar (	12	) null,
abi_number	varchar (	9	) null,
industry_title	varchar (	50	) null,
occupation_title	varchar (	50	) null,
specialty_title	varchar (	50	) null,
sic_code	varchar (	6	) null,
naics_group	varchar (	2	) null,
license_state	varchar (	2	) null,
license_id	varchar (	12	) null,
license_number	varchar (	25	) null,
license_exp_date	varchar (	8	) null,
license_status_code	varchar (	1	) null,
license_refresh_date	varchar (	8	) null,
license_add_date	varchar (	8	) null,
license_change_date	varchar (	8	) null,
year_licensed	varchar (	4	) null,
area_code	varchar (	3	) null,
phone_number	varchar (	7	) null,
source_code	varchar (	5	) null,
move_type	varchar (	1	) null,
move_date	varchar (	6	) null,
record_id	varchar (	9	) null,
eof	varchar (	1	) null,
seq bigint
);

