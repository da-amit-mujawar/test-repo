unload ('select substring(open_date,1,6) as Open_Date,
         count(*) as Count
         from {tablename3}
	     group by substring(open_date,1,6)
         order by substring(open_date,1,6) desc')
to 's3://{s3-internal}{reportname1}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;

unload ('select substring(click_date, 1, 6) as Click_Date,
         count(*) as Count
         from {tablename3}
         where click_date > '' ''
         group by substring(click_date, 1, 6)
         order by substring(click_date, 1, 6) desc')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;

--create table for unload
drop table if exists engaged_domain_100;

create table engaged_domain_100 as
    select domain_name,email_count
    from {tablename2}
    order by email_count desc
    limit 100;

unload (' select * from engaged_domain_100')
to 's3://{s3-internal}{reportname3}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;


unload ('select opened as Times_Opened,count(*) as Number_Of_Opens
         from {tablename3}
	     group by opened
         order by cast(opened as int)')
to 's3://{s3-internal}{reportname4}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;

unload ('select clicked as Times_Clicked,count(*) as Number_Of_Clicks
         from {tablename3}
	     group by clicked
         order by cast(clicked as int)')
to 's3://{s3-internal}{reportname5}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;

unload ('select gst,count(*) as Count
         from {tablename3}
	     group by gst order by GST')
to 's3://{s3-internal}{reportname6}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;

unload ('select optout,count(*) as Count
         from {tablename3}
         where optout= 1
	     group by optout')
to 's3://{s3-internal}{reportname7}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;

unload ('select *
         from {table_job_stats}')
to 's3://{s3-internal}{reportname8}'
iam_role '{iam}'
kms_key_id '{kmskey}'
ENCRYPTED
CSV DELIMITER AS '|'
ALLOWOVERWRITE
header
parallel off
;