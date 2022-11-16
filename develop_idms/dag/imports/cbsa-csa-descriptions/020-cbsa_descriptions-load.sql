--load fixed width file no header
copy {table_cbsa_0}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
fixedwidth 'cbsacode:5,cbtitle:50,filler:23';

--load table, calculate identity field
insert into {table_cbsa_1}
    (cbsacode, cbtitle,iorder)
select cbsacode, cbtitle,
       row_number() over ( order by cbsacode ) as iorder
from {table_cbsa_0};

