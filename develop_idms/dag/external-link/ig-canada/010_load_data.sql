DROP TABLE IF EXISTS {igcanada-raw-table};

CREATE TABLE {igcanada-raw-table}
(
    id INT IDENTITY ENCODE AZ64,
    igcnd_contct VARCHAR(34) ENCODE ZSTD,
    igcnd_coname VARCHAR(30) ENCODE ZSTD,
    igcnd_addr VARCHAR(30) ENCODE ZSTD,
    igcnd_suite VARCHAR(6) ENCODE ZSTD,
    igcnd_city VARCHAR(30) ENCODE ZSTD,
    igcnd_prov VARCHAR(2) ENCODE ZSTD,
    igcnd_provds VARCHAR(20) ENCODE ZSTD,
    igcnd_zip VARCHAR(7) ENCODE ZSTD,
    igcnd_distr VARCHAR(3) ENCODE ZSTD,
    igcnd_dstdsc VARCHAR(20) ENCODE ZSTD,
    igcnd_french VARCHAR(1) ENCODE ZSTD,
    igcnd_popcod VARCHAR(1) ENCODE ZSTD,
    igcnd_popdsc VARCHAR(20) ENCODE ZSTD,
    igcnd_phone VARCHAR(10) ENCODE ZSTD,
    igcnd_credit VARCHAR(1) ENCODE ZSTD,
    igcnd_crddsc VARCHAR(15) ENCODE ZSTD,
    igcnd_indfrm VARCHAR(1) ENCODE ZSTD,
    igcnd_inddsc VARCHAR(10) ENCODE ZSTD,
    igcnd_empsiz VARCHAR(1) ENCODE ZSTD,
    igcnd_empdsc VARCHAR(15) ENCODE ZSTD,
    igcnd_salvol VARCHAR(1) ENCODE ZSTD,
    igcnd_saldsc VARCHAR(26) ENCODE ZSTD,
    igcnd_sic VARCHAR(6) ENCODE ZSTD,
    igcnd_sicds1 VARCHAR(45) ENCODE ZSTD,
    igcnd_frncod VARCHAR(6) ENCODE ZSTD,
    igcnd_frdsc1 VARCHAR(40) ENCODE ZSTD,
    igcnd_frdsc2 VARCHAR(40) ENCODE ZSTD,
    igcnd_frdsc3 VARCHAR(40) ENCODE ZSTD,
    igcnd_frdsc4 VARCHAR(40) ENCODE ZSTD,
    igcnd_frdsc5 VARCHAR(40) ENCODE ZSTD,
    igcnd_frdsc6 VARCHAR(40) ENCODE ZSTD,
    igcnd_iscode VARCHAR(1) ENCODE ZSTD,
    igcnd_iscdsc VARCHAR(15) ENCODE ZSTD,
    igcnd_adsiz VARCHAR(1) ENCODE ZSTD,
    igcnd_addsc VARCHAR(15) ENCODE ZSTD,
    igcnd_prmsic VARCHAR(6) ENCODE ZSTD,
    igcnd_prmdsc VARCHAR(45) ENCODE ZSTD,
    igcnd_ssic1 VARCHAR(6) ENCODE ZSTD,
    igcnd_sicds2 VARCHAR(45) ENCODE ZSTD,
    igcnd_ssic2 VARCHAR(6) ENCODE ZSTD,
    igcnd_sicds3 VARCHAR(45) ENCODE ZSTD,
    igcnd_ssic3 VARCHAR(6) ENCODE ZSTD,
    igcnd_sicds4 VARCHAR(45) ENCODE ZSTD,
    igcnd_ssic4 VARCHAR(6) ENCODE ZSTD,
    igcnd_sicds5 VARCHAR(45) ENCODE ZSTD,
    igcnd_year VARCHAR(4) ENCODE ZSTD,
    igcnd_lastnm VARCHAR(20) ENCODE ZSTD,
    igcnd_frstnm VARCHAR(11) ENCODE ZSTD,
    igcnd_prottl VARCHAR(3) ENCODE ZSTD,
    igcnd_ttlcd VARCHAR(1) ENCODE ZSTD,
    igcnd_ttldsc VARCHAR(14) ENCODE ZSTD,
    igcnd_gendcd VARCHAR(1) ENCODE ZSTD,
    igcnd_gendsc VARCHAR(6) ENCODE ZSTD,
    igcnd_hdbrch VARCHAR(1) ENCODE ZSTD,
    igcnd_hqbdsc VARCHAR(20) ENCODE ZSTD,
    igcnd_prodat VARCHAR(8) ENCODE ZSTD,
    igcnd_fphone VARCHAR(10) ENCODE ZSTD,
    igcnd_cma VARCHAR(3) ENCODE ZSTD,
    igcnd_cmadsc VARCHAR(25) ENCODE ZSTD,
    igcnd_offsiz VARCHAR(1) ENCODE ZSTD,
    igcnd_offdsc VARCHAR(20) ENCODE ZSTD,
    igcnd_ttladd VARCHAR(30) ENCODE ZSTD,
    igcnd_keycod VARCHAR(20) ENCODE ZSTD,
    igcnd_pubprv VARCHAR(1) ENCODE ZSTD,
    igcnd_pubdsc VARCHAR(6) ENCODE ZSTD,
    igcnd_locnum VARCHAR(9) ENCODE ZSTD,
    igcnd_subnum VARCHAR(9) ENCODE ZSTD,
    igcnd_ultnum VARCHAR(9) ENCODE ZSTD,
    igcnd_stock VARCHAR(1) ENCODE ZSTD,
    igcnd_stkdsc VARCHAR(9) ENCODE ZSTD,
    igcnd_ticker VARCHAR(6) ENCODE ZSTD,
    igcnd_empsdt VARCHAR(6) ENCODE ZSTD,
    igcnd_slsvdt VARCHAR(9) ENCODE ZSTD,
    igcnd_pactem VARCHAR(6) ENCODE ZSTD,
    igcnd_pactsl VARCHAR(9) ENCODE ZSTD,
    igcnd_pempsz VARCHAR(1) ENCODE ZSTD,
    igcnd_pemdsc VARCHAR(15) ENCODE ZSTD,
    igcnd_psalvl VARCHAR(1) ENCODE ZSTD,
    igcnd_psvdsc VARCHAR(26) ENCODE ZSTD,
    igcnd_scaddr VARCHAR(30) ENCODE ZSTD,
    igcnd_sccity VARCHAR(30) ENCODE ZSTD,
    igcnd_scprov VARCHAR(2) ENCODE ZSTD,
    igcnd_sczip VARCHAR(7) ENCODE ZSTD,
    igcnd_naicsc VARCHAR(8) ENCODE ZSTD,
    igcnd_ncsdsc VARCHAR(50) ENCODE ZSTD,
    igcnd_condes VARCHAR(19) ENCODE ZSTD,
    igcnd_pdinfo VARCHAR(19) ENCODE ZSTD,
    igcnd_srtlvl VARCHAR(1) ENCODE ZSTD,
    igcnd_dmde VARCHAR(8) ENCODE ZSTD,
    igcnd_lsmp VARCHAR(8) ENCODE ZSTD,
    igcnd_seqcon VARCHAR(48) ENCODE ZSTD,
    igcnd_expdate VARCHAR(20) ENCODE ZSTD,
    igcnd_prdatef VARCHAR(20) ENCODE ZSTD,
    igcnd_source VARCHAR(10) ENCODE ZSTD,
    filler_1 VARCHAR(10),
    filler_2 VARCHAR(2),
    company_mc VARCHAR(15) ENCODE ZSTD
)
DISTSTYLE KEY
DISTKEY(company_mc)
SORTKEY(company_mc);

COPY {igcanada-raw-table}
(
igcnd_contct ,
igcnd_coname ,
igcnd_addr ,
igcnd_suite ,
igcnd_city ,
igcnd_prov ,
igcnd_provds ,
igcnd_zip ,
igcnd_distr ,
igcnd_dstdsc ,
igcnd_french ,
igcnd_popcod ,
igcnd_popdsc ,
igcnd_phone ,
igcnd_credit ,
igcnd_crddsc ,
igcnd_indfrm ,
igcnd_inddsc ,
igcnd_empsiz ,
igcnd_empdsc  ,
igcnd_salvol ,
igcnd_saldsc ,
igcnd_sic ,
igcnd_sicds1 ,
igcnd_frncod ,
igcnd_frdsc1 ,
igcnd_frdsc2 ,
igcnd_frdsc3 ,
igcnd_frdsc4 ,
igcnd_frdsc5 ,
igcnd_frdsc6 ,
igcnd_iscode ,
igcnd_iscdsc ,
igcnd_adsiz ,
igcnd_addsc ,
igcnd_prmsic ,
igcnd_prmdsc ,
igcnd_ssic1 ,
igcnd_sicds2 ,
igcnd_ssic2 ,
igcnd_sicds3 ,
igcnd_ssic3 ,
igcnd_sicds4 ,
igcnd_ssic4 ,
igcnd_sicds5 ,
igcnd_year ,
igcnd_lastnm ,
igcnd_frstnm ,
igcnd_prottl ,
igcnd_ttlcd ,
igcnd_ttldsc ,
igcnd_gendcd ,
igcnd_gendsc ,
igcnd_hdbrch ,
igcnd_hqbdsc ,
igcnd_prodat ,
igcnd_fphone ,
igcnd_cma ,
igcnd_cmadsc ,
igcnd_offsiz  ,
igcnd_offdsc ,
igcnd_ttladd ,
igcnd_keycod ,
igcnd_pubprv ,
igcnd_pubdsc ,
igcnd_locnum ,
igcnd_subnum  ,
igcnd_ultnum  ,
igcnd_stock ,
igcnd_stkdsc ,
igcnd_ticker ,
igcnd_empsdt ,
igcnd_slsvdt ,
igcnd_pactem ,
igcnd_pactsl ,
igcnd_pempsz ,
igcnd_pemdsc ,
igcnd_psalvl ,
igcnd_psvdsc ,
igcnd_scaddr ,
igcnd_sccity ,
igcnd_scprov ,
igcnd_sczip ,
igcnd_naicsc ,
igcnd_ncsdsc ,
igcnd_condes ,
igcnd_pdinfo ,
igcnd_srtlvl ,
igcnd_dmde ,
igcnd_lsmp ,
igcnd_seqcon ,
igcnd_expdate ,
igcnd_prdatef ,
igcnd_source ,
filler_1 ,
filler_2
)
FROM 's3://{s3-internal}{s3-key}'
IAM_ROLE '{iam}'
ACCEPTINVCHARS
IGNOREBLANKLINES
FIXEDWIDTH
'igcnd_contct:34,
igcnd_coname:30,
igcnd_addr:30,
igcnd_suite:6,
igcnd_city:30,
igcnd_prov:2,
igcnd_provds:20,
igcnd_zip:7,
igcnd_distr:3,
igcnd_dstdsc:20,
igcnd_french:1,
igcnd_popcod:1,
igcnd_popdsc:20,
igcnd_phone:10,
igcnd_credit:1,
igcnd_crddsc:15,
igcnd_indfrm:1,
igcnd_inddsc:10,
igcnd_empsiz:1,
igcnd_empdsc:15,
igcnd_salvol:1,
igcnd_saldsc:26,
igcnd_sic:6,
igcnd_sicds1:45,
igcnd_frncod:6,
igcnd_frdsc1:40,
igcnd_frdsc2:40,
igcnd_frdsc3:40,
igcnd_frdsc4:40,
igcnd_frdsc5:40,
igcnd_frdsc6:40,
igcnd_iscode:1,
igcnd_iscdsc:15,
igcnd_adsiz:1,
igcnd_addsc:15,
igcnd_prmsic:6,
igcnd_prmdsc:45,
igcnd_ssic1:6,
igcnd_sicds2:45,
igcnd_ssic2:6,
igcnd_sicds3:45,
igcnd_ssic3:6,
igcnd_sicds4:45,
igcnd_ssic4:6,
igcnd_sicds5:45,
igcnd_year:4,
igcnd_lastnm:20,
igcnd_frstnm:11,
igcnd_prottl:3,
igcnd_ttlcd:1,
igcnd_ttldsc:14,
igcnd_gendcd:1,
igcnd_gendsc:6,
igcnd_hdbrch:1,
igcnd_hqbdsc:20,
igcnd_prodat:8,
igcnd_fphone:10,
igcnd_cma:3,
igcnd_cmadsc:25,
igcnd_offsiz:1,
igcnd_offdsc:20,
igcnd_ttladd:30,
igcnd_keycod:20,
igcnd_pubprv:1,
igcnd_pubdsc:6,
igcnd_locnum:9,
igcnd_subnum:9,
igcnd_ultnum:9,
igcnd_stock:1,
igcnd_stkdsc:9,
igcnd_ticker:6,
igcnd_empsdt:6,
igcnd_slsvdt:9,
igcnd_pactem:6,
igcnd_pactsl:9,
igcnd_pempsz:1,
igcnd_pemdsc:15,
igcnd_psalvl:1,
igcnd_psvdsc:26,
igcnd_scaddr:30,
igcnd_sccity:30,
igcnd_scprov:2,
igcnd_sczip:7,
igcnd_naicsc:8,
igcnd_ncsdsc:50,
igcnd_condes:19,
igcnd_pdinfo:19,
igcnd_srtlvl:1,
igcnd_dmde:8,
igcnd_lsmp:8,
igcnd_seqcon:48,
igcnd_expdate:20,
igcnd_prdatef:20,
igcnd_source:10,
filler_1:10,
filler_2:2';
 
