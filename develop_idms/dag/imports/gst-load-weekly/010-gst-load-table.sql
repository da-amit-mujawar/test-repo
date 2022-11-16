drop table if exists gst_suppressionemails_tobedropped;
create table  gst_suppressionemails_tobedropped  (
emailaddress  varchar(80) ,
refresh_yymmdd varchar(6) ,
suppression_type varchar(1) ,
gst_extent varchar(2) ,
md5_extent varchar(2) ,
old_master_indicator varchar(1) ,
gst_source_code_indicator varchar(20) ,
gst_source_counter varchar(5) ,
source_bits varchar(1000)
);

copy gst_suppressionemails_tobedropped
from 's3://{s3-axle-gold}/{s3-key1}/'
iam_role '{iam}'
delimiter '\t' gzip acceptinvchars ignoreheader 1 ignoreblanklines removequotes maxerror 1000;

/*
rule: when doing weekly gst suppressions,
there is a logic that looks at the [suppression_type] field.
records in the gst file where [suppression_type] = r  (responders) should not be suppressed.
*/

create table {tablename2} as (select trim(upper(emailaddress)) as emailaddress,
                                upper(suppression_type) as suppression_type
                                from gst_suppressionemails_tobedropped
                                where upper(suppression_type) <> 'R' and
                                (emailaddress is not null or trim(emailaddress) <>''))
;
drop table if exists {tablename1};

alter table {tablename2}  rename to {tablename1};