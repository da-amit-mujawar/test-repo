unload ('select o.* from (SELECT id,
                            lems,
                            Last_Transaction_Overall_Days,
                            TOT_PARTS,
                            TOT_DLRS_12M,
                            TOT_TRNS_12M,
                            tot_dlrs_12m_avg,
                            Last_Transaction_Catalog_Days,
                            CAT_PARTS,
                            CAT_DLRS_12M,
                            CAT_TRNS_12M,
                            cat_dlrs_12m_avg,
                            Last_Transaction_Web_Days,
                            WEB_PARTS,
                            WEB_DLRS_12M,
                            WEB_TRNS_12M,
                            Last_Transaction_Retail_Days,
                            RTL_PARTS,
                            RTL_DLRS_12M,
                            RTL_TRNS_12M,
                            Recency_SC01_Days,
                            SC01_DLRS_12M,
                            sc01_dlrs_12m_avg,
                            SC01_TRNS_12M,
                            SC01_TRNS_13P,
                            Recency_SC02_Days,
                            SC02_DLRS_12M,
                            sc02_dlrs_12m_avg,
                            SC02_TRNS_12M,
                            Recency_SC03_Days,
                            SC03_DLRS_12M,
                            sc03_dlrs_12m_avg,
                            SC03_TRNS_12M,
                            SC03_TRNS_13P,
                            Recency_SC04_Days,
                            SC04_DLRS_12M,
                            sc04_dlrs_12m_avg,
                            SC04_TRNS_12M,
                            Recency_SC05_Days,
                            SC05_DLRS_12M,
                            sc05_dlrs_12m_avg,
                            SC05_TRNS_12M,
                            Recency_SC06_Days,
                            SC06_DLRS_12M,
                            sc06_dlrs_12m_avg,
                            SC06_TRNS_12M,
                            Recency_SC07_Days,
                            SC07_DLRS_12M,
                            sc07_dlrs_12m_avg,
                            SC07_TRNS_12M,
                            Recency_SC08_Days,
                            SC08_DLRS_12M,
                            sc08_dlrs_12m_avg,
                            SC08_TRNS_12M,
                            Recency_SC09_Days,
                            SC09_DLRS_12M,
                            sc09_dlrs_12m_avg,
                            SC09_TRNS_12M,
                            Recency_SC10_Days,
                            SC10_DLRS_12M,
                            sc10_dlrs_12m_avg,
                            SC10_TRNS_12M,
                            Recency_SC11_Days,
                            SC11_DLRS_12M,
                            sc11_dlrs_12m_avg,
                            SC11_TRNS_12M,
                            Recency_SC12_Days,
                            SC12_DLRS_12M,
                            sc12_dlrs_12m_avg,
                            SC12_TRNS_12M,
                            Recency_SC13_Days,
                            SC13_DLRS_12M,
                            sc13_dlrs_12m_avg,
                            SC13_TRNS_12M,
                            Recency_SC14_Days,
                            SC14_DLRS_12M,
                            SC14_TRNS_12M,
                            Recency_SC15_Days,
                            SC15_DLRS_12M,
                            SC15_TRNS_12M,
                            Recency_SC16_Days,
                            SC16_DLRS_12M,
                            sc16_dlrs_12m_avg,
                            SC16_TRNS_12M,
                            Recency_SC17_Days,
                            SC17_DLRS_12M,
                            sc17_dlrs_12m_avg,
                            SC17_TRNS_12M,
                            Recency_SC18_Days,
                            SC18_DLRS_12M,
                            SC18_TRNS_12M,
                            Recency_SC19_Days,
                            SC19_DLRS_12M,
                            SC19_TRNS_12M,
                            Recency_SC20_Days,
                            SC20_DLRS_12M,
                            SC20_TRNS_12M,
                            SC20_TRNS_13P,
                            Recency_SC21_Days,
                            SC21_DLRS_12M,
                            SC21_TRNS_12M,
                            Recency_SC22_Days,
                            SC22_DLRS_12M,
                            SC22_TRNS_12M,
                            Recency_002_Days,
                            Recency_003_Days,
                            Recency_007_Days,
                            Recency_008_Days,
                            Recency_013_Days,
                            Recency_016_Days,
                            Recency_018_Days,
                            Recency_019_Days,
                            Recency_020_Days,
                            Recency_021_Days,
                            Recency_026_Days,
                            Recency_027_Days,
                            Recency_042_Days,
                            Recency_043_Days,
                            Recency_045_Days,
                            Recency_046_Days,
                            Recency_047_Days,
                            Recency_066_Days,
                            Recency_079_Days,
                            Recency_091_Days,
                            Recency_092_Days,
                            Recency_100_Days,
                            Recency_109_Days,
                            Recency_110_Days,
                            Recency_111_Days,
                            Recency_119_Days,
                            Recency_122_Days,
                            Recency_128_Days,
                            Recency_129_Days 
FROM {tablename1} limit 100) as o')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off;