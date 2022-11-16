--drop table if exists {mailfile_table};
CREATE TABLE IF NOT EXISTS {mailfile_table}
(
    id                       bigint    identity(1, 1),
    keycode                  VARCHAR(1000),
    personal_name            VARCHAR(1000),
    professional_title       VARCHAR(1000),
    business_name            VARCHAR(1000),
    auxilliary_address       VARCHAR(1000),
    secondary_address        VARCHAR(1000),
    primary_address          VARCHAR(1000),
    city                     VARCHAR(1000),
    state                    VARCHAR(1000),
    zip                      VARCHAR(1000),
    zip_4                    VARCHAR(1000),
    package_code             VARCHAR(1000),
    read_timestamp           TIMESTAMP,
    client_id                VARCHAR(1000),
    project_id               VARCHAR(1000),
    CE_Household_ID           bigint,
    CE_Selected_Individual_ID bigint,
    aop_date                  timestamp
);


--30 mins to load without dedupe
--60 mins with dedupe

--added dedupe logic to remove exact dupes that are loaded on different dates
insert into {mailfile_table}
(keycode, personal_name, professional_title, business_name, auxilliary_address,
 secondary_address, primary_address, city, state, zip, zip_4, package_code,
 read_timestamp, client_id, project_id)
select keycode,
       personal_name,
       professional_title,
       business_name,
       auxilliary_address,
       secondary_address,
       primary_address,
       city,
       state,
       zip,
       zip_4,
       package_code,
       max(read_timestamp),
       client_id,
       project_id
from spectrumdb.apogee_mailfiles_silver
where read_timestamp > nvl((select max(read_timestamp) from {mailfile_table}),
                           getdate() - (5 * 365))
group by keycode, personal_name, professional_title, business_name,
         auxilliary_address, secondary_address, primary_address, city, state,
         zip, zip_4, package_code, client_id, project_id;
