DROP TABLE IF EXISTS templayout;
create table templayout
(
    col001 varchar(600)
);

copy templayout
FROM 's3://axle-donorbase-raw-sources/{filename}'
IAM_ROLE '{iam}'
ACCEPTINVCHARS
MAXERROR 20
IGNOREBLANKLINES
DELIMITER '|'
;