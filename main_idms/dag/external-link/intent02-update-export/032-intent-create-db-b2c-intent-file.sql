

drop table if exists {table_intent_b2c};

CREATE TABLE {table_intent_b2c}
(company_mc varchar(15) ENCODE zstd PRIMARY KEY DISTKEY SORTKEY,
main_cat_code int ENCODE zstd,
sub_cat_code int ENCODE az64,
topic varchar(100) ENCODE zstd);


INSERT INTO {table_intent_b2c} (company_mc, main_cat_code, sub_cat_code, topic)
SELECT company_mc, cast(main_cat_code as int), cast(sub_cat_code as int), REPLACE(UPPER(topic),'-',' ')
FROM {table_topic_final}
WHERE upper(company_mc) IN (SELECT upper(company_mc) FROM {table_b2c_link});
--59 min
--8011431359

insert into {table_job_stats}
select 'Load B2C-Link Topic Table',count(*),getdate() from {table_intent_b2c};

