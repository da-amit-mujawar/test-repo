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


INSERT INTO {tablename1}
SELECT 
lems, 
dlx_segments
    FROM (
        SELECT lems, dlx_segments
             , row_number() OVER (PARTITION BY lems ORDER BY lems DESC) rn
        FROM tblMain_{build_id}_{build} where dlx_segments <> ''
    ) t
    WHERE rn = 1;

