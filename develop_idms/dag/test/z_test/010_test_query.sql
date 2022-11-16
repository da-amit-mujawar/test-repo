drop table if exists apogee_promotions_new;

CREATE TABLE IF NOT EXISTS apogee_promotions_new (
	"keycode" VARCHAR(1000),
	"personal_name" VARCHAR(1000),
	"professional_title" VARCHAR(1000),
	"business_name" VARCHAR(1000),
	"auxilliary_address" VARCHAR(1000),
	"secondary_address" VARCHAR(1000),
	"primary_address" VARCHAR(1000),
	"city" VARCHAR(1000),
	"state" VARCHAR(1000),
	"zip" VARCHAR(1000),
	"zip_4" VARCHAR(1000),
	"package_code" VARCHAR(1000),
	"client_id" VARCHAR(1000),
	"project_id" VARCHAR(1000),
	"package_id" VARCHAR(1000),
	"list_id" VARCHAR(1000),
	"list_name" VARCHAR(1000),
	"list_name_2" VARCHAR(1000),
	"merge_key" VARCHAR(1000),
	"quantity" VARCHAR(1000),
	"mailing_type" VARCHAR(1000),
	"lapsed" INT,
	"multis" INT,
	"seeds" INT,
	"house" INT,
	"abacus" INT,
	"wiland" INT,
	"apogee" INT
 );

COPY apogee_promotions_new
--idms-2722-nessy-apogee/test/redshift-silver-tables-load/silver/promotions/Client_id=PIH/
FROM 's3://idms-2722-nessy-apogee/test/redshift-silver-tables-load/silver/promotions/Client_id=PIH/part-'
IAM_ROLE 'arn:aws:iam::250245842722:role/da-idms-redshift-role'
FORMAT AS PARQUET;