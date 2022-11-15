drop table if exists {splitrecap_table};
CREATE TABLE IF NOT EXISTS {splitrecap_table}
(
    package_id     VARCHAR(1000),
    keycode        VARCHAR(1000),
    list_id        VARCHAR(1000),
    list_name      VARCHAR(1000),
    list_name_2    VARCHAR(1000),
    merge_key      VARCHAR(1000),
    quantity       VARCHAR(1000),
    mailing_type   VARCHAR(1000),
    read_timestamp TIMESTAMP,
	client_id      VARCHAR(1000),
	project_id     VARCHAR(1000),
    lapsed         int,
    multis         int,
    seeds          int,
    house          int,
    abacus         int,
    wiland         int,
    apogee         int
    );

insert into {splitrecap_table}
(package_id, keycode, list_id, list_name, list_name_2, merge_key, quantity,
 mailing_type, read_timestamp, client_id, project_id)
select package_id,
       keycode,
       list_id,
       list_name,
       list_name_2,
       merge_key,
       quantity,
       mailing_type,
       max(read_timestamp),
       client_id,
       project_id
from spectrumdb.apogee_splitrecaps_silver
where read_timestamp >
      nvl((select max(read_timestamp) from {splitrecap_table}),
          getdate() - (5 * 365))
group by package_id, keycode, list_id, list_name, list_name_2, merge_key,
         quantity, mailing_type, client_id, project_id;
