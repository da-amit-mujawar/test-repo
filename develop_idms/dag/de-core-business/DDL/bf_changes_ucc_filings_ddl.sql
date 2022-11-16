DROP TABLE IF EXISTS core_bf.ucc_filings_changes;

CREATE TABLE core_bf.ucc_filings_changes
( 
infogroup_id varchar(15) ENCODE ZSTD,
id Varchar(32) PRIMARY KEY ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD,
internal_record_key varchar(1000) ENCODE ZSTD,
amendment_id Varchar(1000) ENCODE ZSTD,
collateral_types varchar(1000) ENCODE ZSTD,
labels_collateral_types varchar(1000) ENCODE ZSTD,
collateral_types_count int ENCODE AZ64,
expiration_date Varchar(10) ENCODE ZSTD,
filing_date Varchar(10) ENCODE ZSTD,
filing_id Varchar(30) ENCODE ZSTD,
filing_type Varchar(4) ENCODE ZSTD,
labels_filing_type Varchar(1000) ENCODE ZSTD,
jurisdiction Varchar(20) ENCODE ZSTD,
labels_jurisdiction Varchar(1000) ENCODE ZSTD,
secured_party_city Varchar(40) ENCODE ZSTD,
secured_party_key varchar(20) ENCODE ZSTD,
secured_party_name varchar(550) ENCODE ZSTD,
secured_party_postal_code varchar(10) ENCODE ZSTD,
secured_party_state Varchar(2) ENCODE ZSTD,
labels_secured_party_state Varchar(40) ENCODE ZSTD,
secured_party_street Varchar(250) ENCODE ZSTD,
"status" varchar(10) ENCODE ZSTD,
labels_status varchar(10) ENCODE ZSTD
)
diststyle ALL
;