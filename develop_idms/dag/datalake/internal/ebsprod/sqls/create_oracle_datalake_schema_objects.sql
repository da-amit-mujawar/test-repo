----==============================================
---- Need to run a sys/dba user with higher privs
----=============================================
-- Create new schema User for extracts
CREATE USER DATALAKE
  IDENTIFIED BY &password
  DEFAULT TABLESPACE USERS
  TEMPORARY TABLESPACE TEMP
  PROFILE IG_SERVICE
  ACCOUNT UNLOCK
  ENABLE EDITIONS;

-- 1 Role for DATALAKE 
GRANT IG_CONNECT TO DATALAKE;
ALTER USER DATALAKE DEFAULT ROLE ALL;

-- 6 System Privileges for DATALAKE 
GRANT CREATE PROCEDURE TO DATALAKE;
GRANT CREATE SEQUENCE TO DATALAKE;
GRANT CREATE SYNONYM TO DATALAKE;
GRANT CREATE TABLE TO DATALAKE;
GRANT CREATE VIEW TO DATALAKE;
GRANT SELECT ANY TABLE TO DATALAKE;

-- Create Table to hold metadata about the extracts 
CREATE TABLE DATALAKE.SOURCE_EXTRACTS
(
  SCHEMA_NAME               VARCHAR2(128 BYTE)  NOT NULL,
  TABLE_NAME                VARCHAR2(128 BYTE)  NOT NULL,
  DATALAKE_VIEW_NAME        VARCHAR2(128 BYTE),
  EXTRACT_VIEW_NAME         VARCHAR2(128 BYTE),
  INCREMENTAL_CONDITION     VARCHAR2(1000 BYTE),
  START_TIME                DATE,
  END_TIME                  DATE,
  EXTRACT_TYPE              VARCHAR2(20 BYTE),
  STATUS                    VARCHAR2(20 BYTE),
  EXTRACT_ROWS_COUNT        INTEGER,
  TABLE_ROWS_COUNT          INTEGER,
  REDSHIFT_PRE_LOAD_COUNT   INTEGER,
  REDSHIFT_POST_LOAD_COUNT  INTEGER,
  MESSAGE_LOG               VARCHAR2(4000 BYTE)
)
PRIMARY KEY (SCHEMA_NAME, TABLE_NAME)
;
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('APPS', 'RA_CUSTOMERS', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('APPS', 'RA_SALESREPS_ALL','trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('AR', 'AR_COLLECTORS', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('AR', 'RA_CUSTOMER_TRX_ALL', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('AR', 'RA_CUSTOMER_TRX_LINES_ALL', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('AR', 'RA_CUST_TRX_LINE_GL_DIST_ALL', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('AR', 'RA_CUST_TRX_LINE_SALESREPS_ALL', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('AR', 'RA_CUST_TRX_TYPES_ALL', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('GL', 'GL_ACCTS_DESC_TABLE', 'trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('GL', 'GL_CODE_COMBINATIONS', 'trunc(last_update_date) = trunc(sysdate - 1)');
Insert into DATALAKE.SOURCE_EXTRACTS
   (SCHEMA_NAME, TABLE_NAME, INCREMENTAL_CONDITION)
 Values
   ('INV', 'MTL_SYSTEM_ITEMS_B', 'trunc(creation_date) = trunc(sysdate - 1) or trunc(last_update_date) = trunc(sysdate - 1)');
COMMIT;

-- Create a Similar Table as above to hold the Daily Extract Info
CREATE TABLE DAILY_EXTRACT_LOG SELECT * FROM  DATALAKE.SOURCE_EXTRACTS WHERE 1=2;

-- Create DB Directory pointing to local folder to extract files
CREATE DIRECTORY DATALAKE_DIR using '/u02/datalake';
GRANT READ, WRITE ON DIRECTORY DATALAKE_DIR TO DATALAKE;


-- Create a procedure to generate views needed for extract
create or replace PROCEDURE datalake.generate_views (p_schema    IN  VARCHAR2, p_tablename  IN  VARCHAR2, p_extracttype in VARCHAR2 ) AS
  l_cursor      PLS_INTEGER;
  l_rows        PLS_INTEGER;
  l_col_cnt     PLS_INTEGER;
  l_buffer      VARCHAR2(32767);
  l_view_ddl    CLOB;
  l_extract_ddl CLOB;
  l_desc_tab    DBMS_SQL.desc_tab;
  l_refcursor   SYS_REFCURSOR;
  l_src_view    VARCHAR2(30);
  l_ext_view    VARCHAR2(30);
  l_incr_condt  VARCHAR2(1000);
  l_temp        CLOB;
BEGIN
    -- Verify Schema/Table names are valid
    BEGIN 
        select  incremental_condition  into l_incr_condt
        from DATALAKE.SOURCE_EXTRACTS
        where schema_name=upper(p_schema) 
        and table_name=upper(p_tablename);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN 
            dbms_output.put_line('Given Schema or Table name is invalid');
            RAISE; 
    END;
    dbms_output.put_line('Stage 1 - Done');
   
    -- open the cursor  
    OPEN l_refcursor FOR 'select  * from '||p_schema||'.'||p_tablename;
    IF l_refcursor%ISOPEN THEN
     l_cursor := DBMS_SQL.to_cursor_number(l_refcursor);
    ELSE
        RAISE_APPLICATION_ERROR(-20000, 'You must specify a query or a REF CURSOR.');
    END IF;

    DBMS_SQL.describe_columns (l_cursor, l_col_cnt, l_desc_tab);

    FOR i IN 1 .. l_col_cnt LOOP
    DBMS_SQL.define_column(l_cursor, i, l_buffer, 32767 );
    END LOOP;
    dbms_output.put_line('Stage 2 - Done');

    IF LENGTH(p_tablename) < 29 THEN
        l_src_view := 's_'||lower(p_tablename);
        l_ext_view := 'e_'||lower(p_tablename);
    ELSE
        l_src_view := 's_'||lower(substr(p_tablename,1,28));
        l_ext_view := 'e_'||lower(substr(p_tablename,1,28));
    END IF;
    
    l_view_ddl := 'CREATE OR REPLACE VIEW DATALAKE.'||l_src_view||' as select /*+ parallel(4) */ ';
    l_extract_ddl := 'CREATE OR REPLACE VIEW DATALAKE.'||l_ext_view||' as select /*+ parallel(4) */ ';

    FOR i IN 1 .. l_col_cnt LOOP
        IF i > 1 THEN
            l_view_ddl := l_view_ddl || ',';
            l_extract_ddl := l_extract_ddl||'||chr(124)||'; 
        END IF;
        l_view_ddl    := l_view_ddl || l_desc_tab(i).col_name;
        l_extract_ddl := l_extract_ddl||'chr(34)||'; 

        /* Remove Jucks for Varchars */
        IF l_desc_tab(i).col_type IN (DBMS_TYPES.typecode_varchar,
                                      DBMS_TYPES.typecode_varchar2,
                                      DBMS_TYPES.typecode_char,
                                      DBMS_TYPES.typecode_clob,
                                      DBMS_TYPES.typecode_nvarchar2,
                                      DBMS_TYPES.typecode_nchar,
                                      DBMS_TYPES.typecode_nclob) 
        THEN    
            l_extract_ddl := l_extract_ddl || 'ltrim(rtrim(replace(replace(replace(replace('||l_desc_tab(i).col_name||',''|'',''''),''"'',''''),chr(10),''''),chr(13),'''')))';
        ELSE
            l_extract_ddl := l_extract_ddl || l_desc_tab(i).col_name;
        END IF;
        l_extract_ddl := l_extract_ddl || '||chr(34)';
    END LOOP;
    dbms_output.put_line('Stage 3 - Done');

    l_view_ddl    := l_view_ddl || ' from '||p_schema||'.'||p_tablename;
    if (p_extracttype = 'INCREMENTAL') then
        l_view_ddl    :=  l_view_ddl ||' WHERE '||l_incr_condt;
    end if;
    --DBMS_OUTPUT.PUT_LINE (l_view_ddl);
    EXECUTE IMMEDIATE l_view_ddl;
    dbms_output.put_line('Stage 4 - Done' );
    l_temp := ' as extract_data  from '||'DATALAKE.'||l_src_view;
    l_extract_ddl := l_extract_ddl || l_temp;
    EXECUTE IMMEDIATE l_extract_ddl;
    --DBMS_OUTPUT.PUT_LINE (l_extract_ddl);    
    dbms_output.put_line('Stage 5 - Done');
    
    update DATALAKE.SOURCE_EXTRACTS
    set datalake_view_name =  l_src_view,
        extract_view_name =  l_ext_view,
        extract_type = lower(p_extracttype)
    where schema_name=upper(p_schema) 
    and table_name=upper(p_tablename);
    commit;
    dbms_output.put_line('Stage 6 - Done');
    
    DBMS_SQL.close_cursor(l_cursor);
EXCEPTION
  WHEN OTHERS THEN
    IF DBMS_SQL.is_open(l_cursor) THEN
      DBMS_SQL.close_cursor(l_cursor);
    END IF;
    DBMS_OUTPUT.put_line('ERROR: ' || SQLERRM);
    DBMS_OUTPUT.put_line('ERROR: ' || DBMS_UTILITY.format_error_backtrace);
    RAISE;
END generate_views;
/

CREATE OR REPLACE PROCEDURE DATALAKE.generate_redshift_ddl (p_schema    IN  VARCHAR2, p_tablename  IN  VARCHAR2) AS
  l_cursor      PLS_INTEGER;
  l_rows        PLS_INTEGER;
  l_col_cnt     PLS_INTEGER;
  l_buffer      VARCHAR2(32767);
  l_redshift_ddl CLOB;
  l_desc_tab    DBMS_SQL.desc_tab;
  l_refcursor   SYS_REFCURSOR;
  l_temp        CLOB;
BEGIN
    -- Verify Schema/Table names are valid
    BEGIN 
        select  1  into l_rows
        from DATALAKE.SOURCE_EXTRACTS
        where schema_name=upper(p_schema) 
        and table_name=upper(p_tablename);
        EXECUTE IMMEDIATE 'select count(*) from '||upper(p_schema)||'.'||upper(p_tablename) INTO l_rows;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN 
            dbms_output.put_line('Given Schema or Table name is invalid');
            RAISE; 
    END;
    dbms_output.put_line('Stage 1 - Done');
   
    -- open the cursor  
    OPEN l_refcursor FOR 'select  * from '||p_schema||'.'||p_tablename;
    IF l_refcursor%ISOPEN THEN
     l_cursor := DBMS_SQL.to_cursor_number(l_refcursor);
    ELSE
        RAISE_APPLICATION_ERROR(-20000, 'You must specify a query or a REF CURSOR.');
    END IF;

    DBMS_SQL.describe_columns (l_cursor, l_col_cnt, l_desc_tab);

    FOR i IN 1 .. l_col_cnt LOOP
    DBMS_SQL.define_column(l_cursor, i, l_buffer, 32767 );
    END LOOP;
    dbms_output.put_line('Stage 2 - Done');

    l_redshift_ddl := 'CREATE EXTERNAL TABLE spectrumdb.ebsprod_'||lower(p_schema)||'_'||lower(p_tablename)||' (';

    FOR i IN 1 .. l_col_cnt 
    LOOP
        IF l_desc_tab(i).col_type NOT IN (DBMS_TYPES.typecode_varchar,
                                      DBMS_TYPES.typecode_varchar2,
                                      DBMS_TYPES.typecode_char,
                                      DBMS_TYPES.typecode_clob,
                                      DBMS_TYPES.typecode_nvarchar2,
                                      DBMS_TYPES.typecode_nchar,
                                      DBMS_TYPES.typecode_nclob,
                                      DBMS_TYPES.TYPECODE_NUMBER,
                                      DBMS_TYPES.TYPECODE_DATE, 
                                      DBMS_TYPES.TYPECODE_TIMESTAMP
                                      ) 
        THEN
                                      
          dbms_output.put_line('Skipping column - '||l_desc_tab(i).col_name || ' since its of type '||l_desc_tab(i).col_type); 
          CONTINUE;                       
        END IF; 
    
        IF i > 1 THEN
            l_redshift_ddl := l_redshift_ddl || ',' ||CHR(13);
        END IF;
       
        /* Remove Jucks for Varchars */
        IF l_desc_tab(i).col_type IN (DBMS_TYPES.typecode_varchar,
                                      DBMS_TYPES.typecode_varchar2,
                                      DBMS_TYPES.typecode_char,
                                      DBMS_TYPES.typecode_clob,
                                      DBMS_TYPES.typecode_nvarchar2,
                                      DBMS_TYPES.typecode_nchar,
                                      DBMS_TYPES.typecode_nclob) 
        THEN    
            IF l_desc_tab(i).col_max_len > 0 THEN    
                l_redshift_ddl := l_redshift_ddl || l_desc_tab(i).col_name || ' VARCHAR('||(l_desc_tab(i).col_max_len*4)||')';
            ELSE
                l_redshift_ddl := l_redshift_ddl || l_desc_tab(i).col_name || ' VARCHAR(4)';
            END IF;
        ELSIF l_desc_tab(i).col_type IN (DBMS_TYPES.TYPECODE_NUMBER) THEN 
            IF l_desc_tab(i).col_scale > 0 THEN
                l_redshift_ddl := l_redshift_ddl || l_desc_tab(i).col_name || ' FLOAT8';
            ELSE
                l_redshift_ddl := l_redshift_ddl || l_desc_tab(i).col_name || ' INTEGER';
            END IF;
        ELSIF l_desc_tab(i).col_type IN (DBMS_TYPES.TYPECODE_DATE, DBMS_TYPES.TYPECODE_TIMESTAMP ) THEN 
                    l_redshift_ddl := l_redshift_ddl || l_desc_tab(i).col_name || ' DATE';
        END IF;
    END LOOP;
    dbms_output.put_line('Stage 3 - Done');
    l_redshift_ddl := l_redshift_ddl || ') ROW FORMAT SERDE ''org.apache.hadoop.hive.serde2.OpenCSVSerde'' WITH SERDEPROPERTIES ('||CHR(13);
    l_redshift_ddl := l_redshift_ddl || '''separatorChar'' = ''|'','||CHR(13);
    l_redshift_ddl := l_redshift_ddl || '''quoteChar'' = ''"'','||CHR(13);
    l_redshift_ddl := l_redshift_ddl || '''escapeChar'' = ''\\'''||CHR(13);
    l_redshift_ddl := l_redshift_ddl || ') '||CHR(13);
    l_redshift_ddl := l_redshift_ddl || 'STORED AS textfile '||CHR(13);
    l_redshift_ddl := l_redshift_ddl || 'LOCATION ''s3://axle-internal-sources/raw/ebsprod/'||lower(p_schema)||'_'||lower(p_tablename)||'/'' '||CHR(13);
    l_redshift_ddl := l_redshift_ddl || 'TABLE PROPERTIES (''compression_type''=''gzip'',''numRows''='''||l_rows||''');'||CHR(13);

    DBMS_OUTPUT.PUT_LINE (l_redshift_ddl);
    dbms_output.put_line('Stage 4 - Done' );
    DBMS_SQL.close_cursor(l_cursor);
EXCEPTION
  WHEN OTHERS THEN
    IF DBMS_SQL.is_open(l_cursor) THEN
      DBMS_SQL.close_cursor(l_cursor);
    END IF;
    DBMS_OUTPUT.put_line('ERROR: ' || SQLERRM);
    DBMS_OUTPUT.put_line('ERROR: ' || DBMS_UTILITY.format_error_backtrace);
    RAISE;
END generate_redshift_ddl;
/
