DROP TABLE IF EXISTS {tablename1};
CREATE TABLE {tablename1} 
(
    lems varchar(18) ENCODE ZSTD, 
    deluxe_segments varchar(500) ENCODE ZSTD,
    Primary key(lems)
)
DISTSTYLE KEY
DISTKEY(lems)
SORTKEY(lems);