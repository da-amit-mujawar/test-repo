/*
we need to keep it as _new so that we can activate via IDMS.

*/

DROP TABLE IF EXISTS {tablename3}; 

ALTER TABLE {tablename1} RENAME TO {tablename3}; 

DROP TABLE IF EXISTS {tablename2}; 