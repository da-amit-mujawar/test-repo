/*
Email Delta File
This is an extract of email addresses that were present on the previous months DACT
but are no longer on the current DACT.
Included in CR-001 File Name: ‘MYA_MONTHLY_EMAIL_DELTA_CCYYMMDD’ Goes to Meyer outbound mailbox.

18.5 Email Delta File Generated After Each Monthly Process
This file is a new file that will be generated from the monthly process and contain:
    1) all email addresses that were on the previous month’s DACT but are not on the current DACT
    for the alumni files that were sent for the monthly update.
    2) If a school did not send an alumni file, that file will not have to be included in the
    generation of the email delta file. -- cb: no special code needed for this notation
    3) The format of the file will be the MYR EMAIL ADDR and the date the file was created in CCYYMMDD format.
    The MYR EMAIL ADDR will be utilized for this comparison.
This file will be SFTPd to Meyer and Associates outbound mailbox.
The name of the file will be as follows ‘MYA_MONTHLY_EMAIL_DELTA_CCYYMMDD’.

-- IDMS-1559 2021.10.19 CB add schoolid to output
The format of the file will be the MYR EMAIL ADDR, SCHOOL ID, and the CREATED DT
which is the date the file was created in CCYYMMDD format.
*/

-- !!! ONE TIME ONLY -- MUST UPDATE FOR NEXT BUILD and promote to production
-- create email file for previous build
-- drop table if exists meyer_email_{pre_maintable_suffix};
-- select distinct cemail, myrschoolid, {yyyy}{mm}{dd} as date_created
-- into meyer_email_{pre_maintable_suffix}
-- from tblmain_{pre_maintable_suffix};


-- create email file for current build
drop table if exists meyer_email_{build_id}_{build};
select distinct cemail, myrschoolid, {yyyy}{mm}{dd} as date_created
into meyer_email_{build_id}_{build}
from {maintable_name};

-- create delta file:
drop table if exists meyer_email_delta;
select cemail, myrschoolid, date_created
into meyer_email_delta
from meyer_email_{pre_buildid}_{pre_build}     --previous build
where not cemail in (select cemail from {maintable_name});  --this build

--export
unload ('select * from meyer_email_delta')
to 's3://{s3-cust-meyer}{report_delta}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
delimiter as ','
parallel off
allowoverwrite
gzip
;