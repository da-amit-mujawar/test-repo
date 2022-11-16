DROP TABLE IF EXISTS {{ params.tablename }};COMMIT;

CREATE TABLE {{ params.tablename }}
(
ID INT IDENTITY,
cTitle VARCHAR(100),
cTitleCode VARCHAR(7),
cFunctionCode VARCHAR(7)
);COMMIT;