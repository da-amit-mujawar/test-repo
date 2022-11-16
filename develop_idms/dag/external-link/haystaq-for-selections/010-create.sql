DROP TABLE IF EXISTS {tablename1};

CREATE TABLE {tablename1}
(
ID INT  IDENTITY encode az64,
Individual_ID BIGINT  PRIMARY KEY encode raw,   
Haystaq_array varchar(900) encode zstd
)
DISTSTYLE KEY
DISTKEY(Individual_ID)
SORTKEY(Individual_ID);


