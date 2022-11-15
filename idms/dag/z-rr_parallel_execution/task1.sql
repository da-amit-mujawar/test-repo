--FILE 1. Add ApplicationCodes


DROP TABLE IF EXISTS "table1";

CREATE TABLE "table1"
(
    location_id Varchar
(
    65535
),
    business_account_number Varchar
(
    65535
),
    ownership_name Varchar
(
    65535
),
    dba_name Varchar
(
    65535
),
    street_address Varchar
(
    65535
),
    city Varchar
(
    65535
),
    state Varchar
(
    65535
),
    source_zipcode Varchar
(
    65535
),
    business_start_date Varchar
(
    65535
),
    business_end_date Varchar
(
    65535
),
    location_start_date Varchar
(
    65535
),
    location_end_date Varchar
(
    65535
),
    mail_address Varchar
(
    65535
),
    mail_city Varchar
(
    65535
),
    mail_zipcode Varchar
(
    65535
),
    mail_state Varchar
(
    65535
),
    naisc_code Varchar
(
    65535
),
    naisc_code_description Varchar
(
    65535
),
    parking_tax Varchar
(
    65535
),
    transient_occupancy_tax Varchar
(
    65535
),
    lic_code Varchar
(
    65535
),
    lic_code_description Varchar
(
    65535
),
    supervisor_district Varchar
(
    65535
),
    neighborhoods_analysis_boundaries Varchar
(
    65535
),
    business_corridor Varchar
(
    65535
),
    business_location Varchar
(
    65535
));

copy
"table1"
from 's3://idms-2722-playground/rohit_rajput/rr_registered_business_locations_san_francisco.csv'
iam_role '{iam}'
csv quote as '"' ACCEPTINVCHARS EMPTYASNULL IGNOREHEADER 1;


