DROP TABLE IF EXISTS core_bf.benefit_plans;

CREATE TABLE core_bf.benefit_plans
( 
infogroup_id varchar(15) ,
id Varchar(32) PRIMARY KEY,
created_at varchar(50) ,
ack_id Varchar(1000) ,
active_beginning_participants int ,
active_ending_participants int ,
benefit_arrangements Varchar(1000) ,
labels_benefit_arrangements Varchar(10000) ,
benefit_arrangements_count int ,
broker_city Varchar(1000) ,
broker_name varchar(1000) ,
broker_postal_code varchar(10) ,
broker_state Varchar(2) ,
labels_broker_state Varchar(40) ,
broker_street Varchar(1000) ,
carrier varchar(1000) ,
contract_number varchar(1000) ,
effective_date date ,
form_id int ,
funding_arrangements Varchar(1000) ,
labels_funding_arrangements Varchar(10000) ,
funding_arrangements_count int ,
name varchar(1000) ,
row_order int ,
short_form varchar(5) ,
term_lower date ,
term_upper date ,
total_beginning_participants int ,
welfare_benefit_types varchar(1000) ,
labels_welfare_benefit_types varchar(10000) ,
welfare_benefit_types_count int 
)
diststyle ALL
;