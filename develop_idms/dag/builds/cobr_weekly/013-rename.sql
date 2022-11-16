CREATE OR REPLACE PROCEDURE sp_rename()
language plpgsql AS 
$$
DECLARE 
    Day_of_week varchar(20);
BEGIN
    Day_of_week := LEFT(RIGHT('{filename}', LEN('{filename}')-26), 3);
    IF Day_of_week = 'WED'
    THEN
        DROP TABLE IF EXISTS tblMain_14112_201008_ToBeDropped;
        IF EXISTS(SELECT 1 FROM pg_table_def WHERE TABLENAME ='tblmain_14112_201008') 
        THEN 
            ALTER TABLE tblMain_14112_201008 RENAME TO tblMain_14112_201008_ToBeDropped; 
        END IF; 
        ALTER TABLE  tblCobraWeekly881_Final RENAME TO tblMain_14112_201008;
    END IF;

    IF Day_of_week = 'FRI'
    THEN
        DROP TABLE IF EXISTS tblMain_14111_201007_ToBeDropped;
        IF EXISTS(SELECT 1 FROM pg_table_def WHERE TABLENAME ='tblmain_14111_201007') 
        THEN 
            ALTER TABLE tblMain_14111_201007 RENAME TO tblMain_14111_201007_ToBeDropped; 
        END IF; 
        ALTER TABLE  tblCobraWeekly881_Final RENAME TO tblMain_14111_201007;
    END IF;
END
$$
;

call sp_rename();
drop PROCEDURE sp_rename();