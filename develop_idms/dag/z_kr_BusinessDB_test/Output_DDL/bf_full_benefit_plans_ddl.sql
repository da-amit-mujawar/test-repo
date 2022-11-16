DROP TABLE IF EXISTS core_bf.benefit_plans_new;

CREATE TABLE core_bf.benefit_plans_new
( 
infogroup_id varchar(15) ENCODE ZSTD,
Benefit_plan_id Varchar(32) PRIMARY KEY ENCODE ZSTD,
created_at varchar(50) ENCODE ZSTD,
ack_id Varchar(1000) ENCODE ZSTD,
active_beginning_participants int ENCODE ZSTD,
active_ending_participants int ENCODE ZSTD,
benefit_arrangements Varchar(1000) ENCODE ZSTD,
labels_benefit_arrangements Varchar(10000) ENCODE ZSTD,
benefit_arrangements_count int ENCODE ZSTD,
broker_city Varchar(1000) ENCODE ZSTD,
broker_name varchar(1000) ENCODE ZSTD,
broker_postal_code varchar(10) ENCODE ZSTD,
broker_state Varchar(2) ENCODE ZSTD,
labels_broker_state Varchar(40) ENCODE ZSTD,
broker_street Varchar(1000) ENCODE ZSTD,
carrier varchar(1000) ENCODE ZSTD,
contract_number varchar(1000) ENCODE ZSTD,
effective_date date ENCODE ZSTD,
form_id int ENCODE AZ64,
funding_arrangements Varchar(1000) ENCODE ZSTD,
labels_funding_arrangements Varchar(10000) ENCODE ZSTD,
funding_arrangements_count int ENCODE ZSTD,
"name" varchar(1000) ENCODE ZSTD,
row_order int ENCODE ZSTD,
short_form varchar(5) ENCODE ZSTD,
term_lower date ENCODE ZSTD,
term_upper date ENCODE ZSTD,
total_beginning_participants int ENCODE ZSTD,
welfare_benefit_types varchar(1000) ENCODE ZSTD,
labels_welfare_benefit_types varchar(10000) ENCODE ZSTD,
welfare_benefit_types_count int ENCODE ZSTD
)
diststyle ALL
;