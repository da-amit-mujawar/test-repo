--load the returned aop file
DROP TABLE IF EXISTS {aop_table};

CREATE TABLE IF NOT EXISTS {aop_table} (
    "id" bigint,
    "personal_name" VARCHAR(100),
    "primary_address" VARCHAR(100),
	"secondary_address" VARCHAR(100),
	"city" VARCHAR(100),
	"state" VARCHAR(10),
	"zip" VARCHAR(10),
	"zip_4" VARCHAR(10),
	"CE_Household_ID" bigint,
  	"CE_Selected_Individual_ID" bigint
);

COPY {aop_table}
FROM 's3://{s3-aopoutput}/a03.a03_'
IAM_ROLE '{iam}'
IGNOREHEADER 1
DELIMITER '|'
TRUNCATECOLUMNS;

--update the mailfile table by matching on name, address, city, state, zip, zip4
UPDATE {mailfile_table}  mailfiles --apogee_promotions_mailfiles_new
   SET CE_Selected_Individual_ID = p.CE_Selected_Individual_ID,
       CE_Household_ID = p.CE_Household_ID,
       aop_date = getdate()
  FROM {aop_table} p
 WHERE --apogee_promotions_mailfiles_new.id = p.id
       mailfiles.personal_name = p.personal_name and
       mailfiles.primary_address = p.primary_address and
       mailfiles.secondary_address = p.secondary_address and
       mailfiles.city = p.city and
       mailfiles.state = p.state and
       mailfiles.zip = p.zip and
       mailfiles.zip_4 = p.zip_4
;

--update the aop date for none matches
UPDATE {mailfile_table} --apogee_promotions_mailfiles_new
   SET aop_date = GETDATE() 
 WHERE aop_date IS NULL;

