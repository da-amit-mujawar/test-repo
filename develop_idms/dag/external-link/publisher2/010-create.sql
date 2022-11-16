DROP TABLE IF EXISTS {tablename1};

CREATE TABLE {tablename1}(
LEMS VARCHAR(18) PRIMARY KEY encode raw,
Pub2_MS_LMOS int encode az64, 
Pub2_MS_MAGS int encode az64,
Pub2_MS_BOOKS int encode az64,
Pub2_MS_PUB_LMOS int encode zstd,
Pub2_MS_PUB_TOTORDS int encode zstd,
Pub2_MS_PUB_STATUS_ACTIVE int encode zstd,
Pub2_PKCA42 int encode zstd,
Pub2_MS_PUB_AUTORNWL int encode zstd,
Pub2_MS_PUB_STATUS_EXPIRE int encode zstd,
Pub2_MS_PUB_PAYSTAT_PAID int encode az64,
Pub2_MS_PUB_PAYMETH_CASH int encode zstd,
Pub2_source_arr VARCHAR(9) encode zstd,
Pub2_cat1_arr VARCHAR(70) encode zstd,
Pub2_cat2_arr VARCHAR(40) encode zstd,
PUB2SOURCE CHAR(1) encode zstd)
DISTSTYLE KEY
DISTKEY(lems)
SORTKEY(LEMS);