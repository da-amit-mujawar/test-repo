DROP TABLE IF EXISTS {igus-raw-table};
CREATE TABLE {igus-raw-table} 
(
    id INT IDENTITY ENCODE  AZ64,
    igus_contactmrg VARCHAR(30) ENCODE ZSTD,
    igus_coname VARCHAR(30) ENCODE ZSTD,
    igus_prmaddr VARCHAR(30) ENCODE ZSTD,
    igus_prmcity VARCHAR(16) ENCODE ZSTD,
    igus_prmstate VARCHAR(2) ENCODE ZSTD,
    igus_prmzip VARCHAR(5) ENCODE ZSTD,
    igus_prmzip4 VARCHAR(4) ENCODE ZSTD,
    igus_prmzipmrg VARCHAR(10) ENCODE ZSTD,
    igus_prmcrcd VARCHAR(4) ENCODE ZSTD,
    igus_prmstcd VARCHAR(2) ENCODE ZSTD,
    igus_cntycd VARCHAR(3) ENCODE ZSTD,
    igus_cntyds VARCHAR(14) ENCODE ZSTD,
    igus_msacd VARCHAR(4) ENCODE ZSTD ,
    igus_msads VARCHAR(30) ENCODE ZSTD ,
    igus_cbsacd VARCHAR(5) ENCODE ZSTD,
    igus_cbsads VARCHAR(16) ENCODE ZSTD,
    igus_metroind VARCHAR(1) ENCODE ZSTD,
    igus_csacode VARCHAR(3) ENCODE ZSTD,
    igus_csadesc VARCHAR(50) ENCODE ZSTD,
    igus_centrct VARCHAR(6) ENCODE ZSTD,
    igus_blockgrp VARCHAR(1) ENCODE ZSTD,
    igus_latitudeo VARCHAR(10) ENCODE ZSTD,
    igus_longitudeo VARCHAR(11) ENCODE ZSTD,
    igus_matchcd VARCHAR(1) ENCODE ZSTD,
    igus_secaddr VARCHAR(30) ENCODE ZSTD,
    igus_seccity VARCHAR(16) ENCODE ZSTD,
    igus_secstate VARCHAR(2) ENCODE ZSTD,
    igus_secstcd VARCHAR(2) ENCODE ZSTD,
    igus_seczip VARCHAR(5) ENCODE ZSTD,
    igus_seczip4 VARCHAR(4) ENCODE ZSTD,
    igus_seczipmrg VARCHAR(10) ENCODE ZSTD,
    igus_seccrcd VARCHAR(4) ENCODE ZSTD,
    igus_phonenumo VARCHAR(12) ENCODE ZSTD,
    igus_faxnumo VARCHAR(12) ENCODE ZSTD,
    igus_tollfrnumo VARCHAR(12) ENCODE ZSTD,
    igus_webaddr VARCHAR(40) ENCODE ZSTD,
    igus_selsiccd VARCHAR(6) ENCODE ZSTD,
    igus_selsicds VARCHAR(45) ENCODE ZSTD,
    igus_frncd1x VARCHAR(1) ENCODE ZSTD ,
    igus_frnds1 VARCHAR(40) ENCODE ZSTD,
    igus_frncd2x VARCHAR(1) ENCODE ZSTD ,
    igus_frnds2 VARCHAR(40) ENCODE ZSTD,
    igus_frncd3x VARCHAR(1) ENCODE ZSTD ,
    igus_frnds3 VARCHAR(40) ENCODE ZSTD ,
    igus_frncd4x VARCHAR(1) ENCODE ZSTD,
    igus_frnds4 VARCHAR(40) ENCODE ZSTD ,
    igus_frncd5x VARCHAR(1) ENCODE ZSTD,
    igus_frnds5 VARCHAR(40) ENCODE ZSTD,
    igus_frncd6x VARCHAR(1) ENCODE ZSTD,
    igus_frnds6 VARCHAR(40) ENCODE ZSTD,
    igus_trufrnflg VARCHAR(1) ENCODE ZSTD,
    igus_indspccd VARCHAR(1) ENCODE ZSTD ,
    igus_incspcds VARCHAR(40) ENCODE ZSTD,
    igus_prmsiccd VARCHAR(6) ENCODE ZSTD,
    igus_prmsicds VARCHAR(45) ENCODE ZSTD,
    igus_secsiccd1 VARCHAR(6) ENCODE ZSTD,
    igus_secsicds1 VARCHAR(45) ENCODE ZSTD,
    igus_secsiccd2 VARCHAR(6) ENCODE ZSTD,
    igus_secsicds2 VARCHAR(45) ENCODE ZSTD,
    igus_secsiccd3 VARCHAR(6) ENCODE ZSTD,
    igus_secsicds3 VARCHAR(45) ENCODE ZSTD,
    igus_secsiccd4 VARCHAR(6) ENCODE ZSTD,
    igus_secsidds4 VARCHAR(45) ENCODE ZSTD,
    igus_naicscd VARCHAR(8) ENCODE ZSTD,
    igus_naicsds VARCHAR(50) ENCODE ZSTD,
    igus_lempszcd VARCHAR(1) ENCODE ZSTD,
    igus_lempszds VARCHAR(9) ENCODE ZSTD,
    igus_alempsz VARCHAR(5) ENCODE ZSTD,
    igus_cempszcd VARCHAR(1) ENCODE ZSTD ,
    igus_cempszds VARCHAR(9) ENCODE ZSTD,
    igus_acempsz VARCHAR(6) ENCODE ZSTD ,
    igus_modesflg VARCHAR(1) ENCODE ZSTD,
    igus_lsalvolcd VARCHAR(1) ENCODE ZSTD,
    igus_lsalvolds VARCHAR(18) ENCODE ZSTD,
    igus_alslsvol VARCHAR(9) ENCODE ZSTD,
    igus_csalvolcd VARCHAR(1) ENCODE ZSTD,
    igus_csalvolds VARCHAR(18) ENCODE ZSTD ,
    igus_acslsvol VARCHAR(9) ENCODE ZSTD ,
    igus_assetszflg VARCHAR(1) ENCODE ZSTD,
    igus_lastnm VARCHAR(14) ENCODE ZSTD,
    igus_frstnm VARCHAR(11) ENCODE ZSTD,
    igus_salute VARCHAR(2) ENCODE ZSTD,
    igus_gendcd VARCHAR(1) ENCODE ZSTD,
    igus_prottl VARCHAR(3) ENCODE ZSTD,
    igus_ttlcd VARCHAR(1) ENCODE ZSTD,
    igus_ttlds VARCHAR(18) ENCODE ZSTD,
    igus_ethniccode VARCHAR(2) ENCODE ZSTD,
    igus_ehnicddes VARCHAR(40) ENCODE ZSTD,
    igus_keycd VARCHAR(20) ENCODE ZSTD ,
    igus_ttladdr VARCHAR(30) ENCODE ZSTD ,
    igus_abinum VARCHAR(9) ENCODE ZSTD,
    igus_subnum VARCHAR(9) ENCODE ZSTD,
    igus_ultnum VARCHAR(9) ENCODE ZSTD,
    igus_sitenum VARCHAR(9) ENCODE ZSTD,
    igus_busstatcd VARCHAR(1) ENCODE ZSTD,
    igus_busstatds VARCHAR(20) ENCODE ZSTD,
    igus_pubprvcd VARCHAR(1) ENCODE ZSTD,
    igus_stockexcd VARCHAR(1) ENCODE ZSTD,
    igus_stockexds VARCHAR(6) ENCODE ZSTD,
    igus_ticker VARCHAR(6) ENCODE ZSTD,
    igus_pubfiling VARCHAR(1) ENCODE ZSTD,
    igus_fortunernk VARCHAR(4) ENCODE ZSTD,
    igus_indfirmcd VARCHAR(1) ENCODE ZSTD,
    igus_indfirmds VARCHAR(13) ENCODE ZSTD,
    igus_yrsicadded VARCHAR(6) ENCODE ZSTD,
    igus_yrfirstapp VARCHAR(4) ENCODE ZSTD,
    igus_yrestblshd VARCHAR(4) ENCODE ZSTD,
    igus_ypcode VARCHAR(5) ENCODE ZSTD,
    igus_transdate VARCHAR(6) ENCODE ZSTD,
    igus_transtype VARCHAR(1) ENCODE ZSTD,
    igus_telresdate VARCHAR(6) ENCODE ZSTD,
    igus_callstatcd VARCHAR(1) ENCODE ZSTD,
    igus_callstatds VARCHAR(25) ENCODE ZSTD,
    igus_crdscrcd VARCHAR(2) ENCODE ZSTD,
    igus_crdscrds VARCHAR(24) ENCODE ZSTD,
    igus_actcrdscr VARCHAR(3) ENCODE ZSTD,
    igus_adsizecd VARCHAR(1) ENCODE ZSTD,
    igus_adsizeds VARCHAR(15) ENCODE ZSTD,
    igus_offsizcd VARCHAR(1) ENCODE ZSTD,
    igus_offsizds VARCHAR(16) ENCODE ZSTD,
    igus_popcd VARCHAR(1) ENCODE ZSTD,
    igus_popds VARCHAR(20) ENCODE ZSTD,
    igus_workhome VARCHAR(1) ENCODE ZSTD ,
    igus_ownleasecd VARCHAR(1) ENCODE ZSTD ,
    igus_sqfootcd VARCHAR(1) ENCODE ZSTD,
    igus_sqfootds VARCHAR(15) ENCODE ZSTD,
    igus_raddisto VARCHAR(5) ENCODE ZSTD,
    igus_actmultitn VARCHAR(7) ENCODE ZSTD,
    igus_multitntcd VARCHAR(1) ENCODE ZSTD,
    igus_multitntds VARCHAR(9) ENCODE ZSTD,
    igus_bldmultitn VARCHAR(7) ENCODE ZSTD,
    igus_fleetcd VARCHAR(1) ENCODE ZSTD,
    igus_fleetds VARCHAR(40) ENCODE ZSTD,
    igus_affluent VARCHAR(1) ENCODE ZSTD,
    igus_bgbusiness VARCHAR(1) ENCODE ZSTD,
    igus_femowner VARCHAR(1) ENCODE ZSTD,
    igus_bussizechg VARCHAR(1) ENCODE ZSTD,
    igus_highincex VARCHAR(1) ENCODE ZSTD,
    igus_hightech VARCHAR(1) ENCODE ZSTD,
    igus_mdbusentr VARCHAR(1) ENCODE ZSTD,
    igus_smbusentr VARCHAR(1) ENCODE ZSTD,
    igus_maildlptbo VARCHAR(12) ENCODE ZSTD ,
    igus_endorseln VARCHAR(30) ENCODE ZSTD,
    igus_bagnum VARCHAR(9) ENCODE ZSTD,
    igus_bundlenum VARCHAR(9) ENCODE ZSTD,
    igus_pslntravel VARCHAR(7) ENCODE ZSTD,
    igus_teraddr VARCHAR(30) ENCODE ZSTD,
    igus_tercity VARCHAR(16) ENCODE ZSTD,
    igus_terstate VARCHAR(2) ENCODE ZSTD,
    igus_terzip VARCHAR(5) ENCODE ZSTD,
    igus_terzip4 VARCHAR(4) ENCODE ZSTD,
    igus_terzipmrg VARCHAR(10) ENCODE ZSTD,
    igus_tercrcd VARCHAR(4) ENCODE ZSTD,
    igus_wcperctage VARCHAR(4) ENCODE ZSTD,
    igus_wcind VARCHAR(1) ENCODE ZSTD,
    igus_prddate VARCHAR(8) ENCODE ZSTD,
    igus_seqnum VARCHAR(10) ENCODE ZSTD,
    igus_expdate VARCHAR(20) ENCODE ZSTD,
    igus_prdatef VARCHAR(20) ENCODE ZSTD,
    igus_source VARCHAR(10) ENCODE ZSTD,
    igus_booknum VARCHAR(5) ENCODE ZSTD,
    igus_govsegcd VARCHAR(1) ENCODE ZSTD,
    igus_foreignpar VARCHAR(1) ENCODE ZSTD,
    igus_imptexptcd VARCHAR(1) ENCODE ZSTD,
    igus_frncd1 VARCHAR(3) ENCODE ZSTD,
    igus_frncd2 VARCHAR(3) ENCODE ZSTD,
    igus_frncd3 VARCHAR(3) ENCODE ZSTD,
    igus_frncd4 VARCHAR(3) ENCODE ZSTD,
    igus_frncd5 VARCHAR(3) ENCODE ZSTD,
    igus_frncd6 VARCHAR(3) ENCODE ZSTD,
    igus_execemail VARCHAR(60) ENCODE ZSTD,
    filler_1 VARCHAR(1),
    filler_2 VARCHAR(2),
    company_mc VARCHAR(15) ENCODE ZSTD,
    igus_prmsiccode4 VARCHAR(4) ENCODE ZSTD,
    igus_hasphone VARCHAR(1) ENCODE ZSTD,
    igus_prmsiccode2 VARCHAR(2) ENCODE ZSTD
)
DISTSTYLE KEY
DISTKEY(company_mc)
SORTKEY(company_mc);

COPY {igus-raw-table} 
(
    igus_contactmrg ,
    igus_coname,
    igus_prmaddr ,
    igus_prmcity,
    igus_prmstate,
    igus_prmzip ,
    igus_prmzip4,
    igus_prmzipmrg ,
    igus_prmcrcd ,
    igus_prmstcd,
    igus_cntycd ,
    igus_cntyds ,
    igus_msacd ,
    igus_msads ,
    igus_cbsacd ,
    igus_cbsads ,
    igus_metroind ,
    igus_csacode ,
    igus_csadesc ,
    igus_centrct,
    igus_blockgrp ,
    igus_latitudeo ,
    igus_longitudeo ,
    igus_matchcd ,
    igus_secaddr ,
    igus_seccity ,
    igus_secstate,
    igus_secstcd ,
    igus_seczip ,
    igus_seczip4 ,
    igus_seczipmrg ,
    igus_seccrcd ,
    igus_phonenumo ,
    igus_faxnumo ,
    igus_tollfrnumo ,
    igus_webaddr ,
    igus_selsiccd ,
    igus_selsicds ,
    igus_frncd1x ,
    igus_frnds1 ,
    igus_frncd2x ,
    igus_frnds2,
    igus_frncd3x ,
    igus_frnds3 ,
    igus_frncd4x ,
    igus_frnds4,
    igus_frncd5x ,
    igus_frnds5 ,
    igus_frncd6x ,
    igus_frnds6 ,
    igus_trufrnflg ,
    igus_indspccd ,
    igus_incspcds ,
    igus_prmsiccd ,
    igus_prmsicds ,
    igus_secsiccd1 ,
    igus_secsicds1 ,
    igus_secsiccd2 ,
    igus_secsicds2 ,
    igus_secsiccd3 ,
    igus_secsicds3 ,
    igus_secsiccd4 ,
    igus_secsidds4 ,
    igus_naicscd ,
    igus_naicsds ,
    igus_lempszcd ,
    igus_lempszds ,
    igus_alempsz ,
    igus_cempszcd ,
    igus_cempszds ,
    igus_acempsz ,
    igus_modesflg ,
    igus_lsalvolcd ,
    igus_lsalvolds ,
    igus_alslsvol ,
    igus_csalvolcd ,
    igus_csalvolds ,
    igus_acslsvol ,
    igus_assetszflg ,
    igus_lastnm ,
    igus_frstnm ,
    igus_salute ,
    igus_gendcd ,
    igus_prottl ,
    igus_ttlcd ,
    igus_ttlds ,
    igus_ethniccode ,
    igus_ehnicddes ,
    igus_keycd ,
    igus_ttladdr ,
    igus_abinum ,
    igus_subnum ,
    igus_ultnum ,
    igus_sitenum ,
    igus_busstatcd ,
    igus_busstatds ,
    igus_pubprvcd ,
    igus_stockexcd ,
    igus_stockexds ,
    igus_ticker ,
    igus_pubfiling ,
    igus_fortunernk ,
    igus_indfirmcd ,
    igus_indfirmds ,
    igus_yrsicadded ,
    igus_yrfirstapp ,
    igus_yrestblshd ,
    igus_ypcode ,
    igus_transdate ,
    igus_transtype ,
    igus_telresdate ,
    igus_callstatcd ,
    igus_callstatds ,
    igus_crdscrcd ,
    igus_crdscrds ,
    igus_actcrdscr ,
    igus_adsizecd ,
    igus_adsizeds ,
    igus_offsizcd ,
    igus_offsizds ,
    igus_popcd ,
    igus_popds ,
    igus_workhome ,
    igus_ownleasecd ,
    igus_sqfootcd ,
    igus_sqfootds ,
    igus_raddisto ,
    igus_actmultitn ,
    igus_multitntcd ,
    igus_multitntds,
    igus_bldmultitn ,
    igus_fleetcd ,
    igus_fleetds ,
    igus_affluent ,
    igus_bgbusiness ,
    igus_femowner ,
    igus_bussizechg ,
    igus_highincex ,
    igus_hightech ,
    igus_mdbusentr,
    igus_smbusentr,
    igus_maildlptbo ,
    igus_endorseln,
    igus_bagnum,
    igus_bundlenum,
    igus_pslntravel,
    igus_teraddr,
    igus_tercity,
    igus_terstate,
    igus_terzip,
    igus_terzip4,
    igus_terzipmrg,
    igus_tercrcd,
    igus_wcperctage,
    igus_wcind,
    igus_prddate,
    igus_seqnum ,
    igus_expdate,
    igus_prdatef,
    igus_source,
    igus_booknum,
    igus_govsegcd,
    igus_foreignpar,
    igus_imptexptcd,
    igus_frncd1,
    igus_frncd2,
    igus_frncd3,
    igus_frncd4,
    igus_frncd5,
    igus_frncd6,
    igus_execemail ,
    filler_1,
    filler_2
)
FROM 's3://{s3-internal}{s3-key}'
IAM_ROLE '{iam}'
IGNOREBLANKLINES
ACCEPTINVCHARS
FIXEDWIDTH
'igus_contactmrg:30,
igus_coname:30,
igus_prmaddr:30,
igus_prmcity:16,
igus_prmstate:2,
igus_prmzip:5,
igus_prmzip4:4,
igus_prmzipmrg:10,
igus_prmcrcd:4,
igus_prmstcd:2,
igus_cntycd:3,
igus_cntyds:14,
igus_msacd:4,
igus_msads:30,
igus_cbsacd:5,
igus_cbsads:16,
igus_metroind:1,
igus_csacode:3,
igus_csadesc:50,
igus_centrct:6,
igus_blockgrp:1,
igus_latitudeo:10,
igus_longitudeo:11,
igus_matchcd:1,
igus_secaddr:30,
igus_seccity:16,
igus_secstate:2,
igus_secstcd:2,
igus_seczip:5,
igus_seczip4:4,
igus_seczipmrg:10,
igus_seccrcd:4,
igus_phonenumo:12,
igus_faxnumo:12,
igus_tollfrnumo:12,
igus_webaddr:40,
igus_selsiccd:6,
igus_selsicds:45,
igus_frncd1x:1,
igus_frnds1:40,
igus_frncd2x:1,
igus_frnds2:40,
igus_frncd3x:1,
igus_frnds3:40,
igus_frncd4x:1,
igus_frnds4:40,
igus_frncd5x:1,
igus_frnds5:40,
igus_frncd6x:1,
igus_frnds6:40,
igus_trufrnflg:1,
igus_indspccd:1,
igus_incspcds:40,
igus_prmsiccd:6,
igus_prmsicds:45,
igus_secsiccd1:6,
igus_secsicds1:45,
igus_secsiccd2:6,
igus_secsicds2:45,
igus_secsiccd3:6,
igus_secsicds3:45,
igus_secsiccd4:6,
igus_secsidds4:45,
igus_naicscd:8,
igus_naicsds:50,
igus_lempszcd:1,
igus_lempszds:9,
igus_alempsz:5,
igus_cempszcd:1,
igus_cempszds:9,
igus_acempsz:6,
igus_modesflg:1,
igus_lsalvolcd:1,
igus_lsalvolds:18,
igus_alslsvol:9,
igus_csalvolcd:1,
igus_csalvolds:18,
igus_acslsvol:9,
igus_assetszflg:1,
igus_lastnm:14,
igus_frstnm:11,
igus_salute:2,
igus_gendcd:1,
igus_prottl:3,
igus_ttlcd:1,
igus_ttlds:18,
igus_ethniccode:2,
igus_ehnicddes:40,
igus_keycd:20,
igus_ttladdr:30,
igus_abinum:9,
igus_subnum:9,
igus_ultnum:9,
igus_sitenum:9,
igus_busstatcd:1,
igus_busstatds:20,
igus_pubprvcd:1,
igus_stockexcd:1,
igus_stockexds:6,
igus_ticker:6,
igus_pubfiling:1,
igus_fortunernk:4,
igus_indfirmcd:1,
igus_indfirmds:13,
igus_yrsicadded:6,
igus_yrfirstapp:4,
igus_yrestblshd:4,
igus_ypcode:5,
igus_transdate:6,
igus_transtype:1,
igus_telresdate:6,
igus_callstatcd:1,
igus_callstatds:25,
igus_crdscrcd:2,
igus_crdscrds:24,
igus_actcrdscr:3,
igus_adsizecd:1,
igus_adsizeds:15,
igus_offsizcd:1,
igus_offsizds:16,
igus_popcd:1,
igus_popds:20,
igus_workhome:1,
igus_ownleasecd:1,
igus_sqfootcd:1,
igus_sqfootds:15,
igus_raddisto:5,
igus_actmultitn:7,
igus_multitntcd:1,
igus_multitntds:9,
igus_bldmultitn:7,
igus_fleetcd:1,
igus_fleetds:40,
igus_affluent:1,
igus_bgbusiness:1,
igus_femowner:1,
igus_bussizechg:1,
igus_highincex:1,
igus_hightech:1,
igus_mdbusentr:1,
igus_smbusentr:1,
igus_maildlptbo:12,
igus_endorseln:30,
igus_bagnum:9,
igus_bundlenum:9,
igus_pslntravel:7,
igus_teraddr:30,
igus_tercity:16,
igus_terstate:2,
igus_terzip:5,
igus_terzip4:4,
igus_terzipmrg:10,
igus_tercrcd:4,
igus_wcperctage:4,
igus_wcind:1,
igus_prddate:8,
igus_seqnum:10,
igus_expdate:20,
igus_prdatef:20,
igus_source:10,
igus_booknum:5,
igus_govsegcd:1,
igus_foreignpar:1,
igus_imptexptcd:1,
igus_frncd1:3,
igus_frncd2:3,
igus_frncd3:3,
igus_frncd4:3,
igus_frncd5:3,
igus_frncd6:3,
igus_execemail:60,
filler_1:1,
filler_2:2';