--load fixed width file no header
copy {table_csa_0}
from 's3://{s3-internal}{s3-key2}'
iam_role '{iam}'
fixedwidth 'csacode:3,csa_name:60,filler:52';

--load table, create identity field
insert into {table_csa_1}
    (csacode, csa_name,iorder)
select csacode, csa_name,
       row_number() over ( order by csacode ) as iorder
from {table_csa_0};
