CREATE OR REPLACE PROCEDURE sp_CloseNotificationJob (
    MainTableName IN VARCHAR(50),
    UpdatedResult INOUT int
)
AS $$
DECLARE
  strSQL VARCHAR(max) ;
  intRowcount int;
BEGIN
  --call sp_CloseNotificationJob('tblMain_reju_tobedropped',0);

  --Counting affected rows are not working. Removed for now. Reju M 2021.04.14
  --DROP TABLE IF EXISTS closejobcounts_tobedropped;
  --CREATE TABLE closejobcounts_tobedropped (tablename varchar(200), rowsaffected bigint);

  strSQL = 'UPDATE ' + MainTableName + ' SET cInclude = ''Y'' WHERE cInclude = ''M'' ;  ';
  EXECUTE strSQL	;


--   INSERT INTO closejobcounts_tobedropped (tablename, rowsaffected)
--   WITH inserted_result AS (
--            SELECT i.tbl        AS tableid,
--                   SUM(i.rows)  AS total_affected_rows
--            FROM   STV_SESSIONS s
--                   INNER JOIN stl_query q
--                        ON  s.process = q.pid
--                   INNER JOIN stl_insert i
--                        ON  i.query = q.query
--            WHERE  i.rows > 0
--            GROUP BY
--                   i.tbl
--        )
--   SELECT DISTINCT t.name AS tablename,
--          total_affected_rows
--   FROM   inserted_result ir
--          INNER JOIN STV_TBL_PERM t
--               ON  t.id = ir.tableid;
--
--   SELECT rowsaffected INTO intRowcount from closejobcounts_tobedropped;

END;
$$ LANGUAGE plpgsql;
/



