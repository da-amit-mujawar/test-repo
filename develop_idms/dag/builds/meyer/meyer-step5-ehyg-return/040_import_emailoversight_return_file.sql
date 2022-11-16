--return
drop table if exists {table1-myr};

create table {table1-myr} (
email      varchar(100) ,
ValidationStatusId varchar(50),
ValidationStatus   varchar(50),
EmailDomainGroupId varchar(50),
EmailDomainGroup   varchar(50)
);

copy {table1-myr}
from 's3://{s3-aopoutput}/{emailoversight-prefix}_d045000_myr_'
iam_role '{iam}'
csv
ignoreheader 1
acceptinvchars
;








 
