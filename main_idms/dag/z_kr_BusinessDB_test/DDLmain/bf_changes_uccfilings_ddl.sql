DROP TABLE IF EXISTS core_bf.ucc_filings;

CREATE TABLE core_bf.ucc_filings
( 
infogroup_id varchar(15),
id Varchar(32) PRIMARY KEY,
created_at varchar(50),
internal_record_key varchar(1000),
amendment_id Varchar(1000),
collateral_types varchar(1000),
labels_collateral_types varchar(1000),
collateral_types_count int,
expiration_date Varchar(10),
filing_date Varchar(10),
filing_id Varchar(30),
filing_type Varchar(4),
labels_filing_type Varchar(1000),
jurisdiction Varchar(20),
labels_jurisdiction Varchar(1000),
secured_party_city Varchar(40),
secured_party_key varchar(20),
secured_party_name varchar(550),
secured_party_postal_code varchar(10),
secured_party_state Varchar(2),
labels_secured_party_state Varchar(40),
secured_party_street Varchar(250),
status varchar(10),
labels_status varchar(10)
)
diststyle ALL
;