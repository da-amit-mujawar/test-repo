CREATE OR REPLACE PROCEDURE Upd_Meyer_Counttables(ln_orderID VARCHAR) AS
$$
DECLARE
    v_sql    VARCHAR(512);
    lv_tName VARCHAR(50);
BEGIN
    BEGIN
        lv_tName = 'Count_' + ln_orderID;
        SELECT tablename
          INTO lv_tName
          FROM pg_tables
         WHERE LOWER(tablename) = LOWER(lv_tName);
        IF NOT FOUND THEN
            lv_tName = 'PreviousCount_' + ln_orderID;
        END IF;
    END;

    -- Add OLD_MCDINDIVIDUALID column if does not exist
    DECLARE
        V_Tbl      VARCHAR(100);
        ln_count   INT;
        lv_tempcol INT;
    BEGIN
        SELECT 1
          INTO lv_tempcol
          FROM pg_table_def
         WHERE LOWER(tablename) = LOWER(lv_tName)
           AND "column" = 'old_mcdindividualid';
        IF NOT FOUND THEN
            V_Tbl = 'ALTER TABLE ' + lv_tName +
                    ' ADD COLUMN old_mcdindividualid varchar(20)';
            EXECUTE V_Tbl;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'SQL ERROR %',SQLERRM;
    END;

    --     Generate Update Script Table
    v_sql = 'UPDATE ' + lv_tName +
            '   SET mcdindividualid     = new_mcdindividualid, ' +
            '       old_mcdindividualid = NVL(' + lv_tName +
            '.old_mcdindividualid, mcdindividualid)' +
            '  FROM meyer_mcd_xref ' +
            ' WHERE mcdindividualid = meyer_mcd_xref.old_mcdindividualid';
    EXECUTE v_sql;
    RAISE INFO 'SQL: [%] ',v_sql;

EXCEPTION
    WHEN OTHERS THEN
        RAISE INFO 'An exception Error [%]', SQLERRM;
END;
$$ LANGUAGE Plpgsql
    SECURITY DEFINER;


GRANT EXECUTE ON PROCEDURE Upd_Meyer_Counttables(varchar) TO airflow_app;
