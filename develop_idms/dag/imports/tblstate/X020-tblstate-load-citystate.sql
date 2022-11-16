--load load fixed width file no header
copy {table_input1}
from 's3://{s3-internal}{s3-key1}'
iam_role '{iam}'
fixedwidth 'filler1:1,zipcode:5,citystatekey:6,zipclassificationcode:1,citystatename:28,citystatenameabbr:13,
    citystatenamefacilitycode:1,citystatemailingnameindicator:1,preferredlastlinecitystatekey:6,
    preferredlastlinecitystatename:28,citydeliveryindicator:1,crrs_merged5digitind:1,uniquezipcode:1,
    financenumber:6,stateabbr:2,countynumber:3,countyname:25'
ignoreheader 1;


-- remove empty values in join columns
delete from {table_input1}
 where citystatekey = '' or stateabbr = '' or zipcode = '';
