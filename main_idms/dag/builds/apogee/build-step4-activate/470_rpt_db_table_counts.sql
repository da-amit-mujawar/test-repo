/*
store the result in redshift table for each databaseid + buildid
generate pivot report that compares past two builds


 --20221003 CB Remove FEC from build

*/
--get Table counts
unload('
    select 0 as Sort, ''DA Consumer Universe'' as TableDescription, count(*) CurrentRecords, count(distinct company_ID) CurrentHouseholds, count(distinct Individual_ID) CurrentIndividuals from {maintable_name}
    union
    select 6, ''DA Transactional Data (mGen)'', count(*), count(distinct company_ID), 0 from tblChild2_{build_id}_{build}
    union
    select 2, ''DA Donor Data Gifts: Apogee NonProfits'', count(*), count(distinct company_ID), count(distinct Individual_ID) from tblChild3_{build_id}_{build}
    union
/*    select 4, ''DA Donor Data Gifts: Apogee FEC'', count(*), count(distinct company_ID), count(distinct Individual_ID) from tblChild4_{build_id}_{build}
    union */
    select 1, ''DA Donor Data HH RollUps: Apogee NonProfits'', count(*), count(distinct company_ID), 0 from tblChild1_{build_id}_{build}
    union
/*    select 3, ''DA Donor Data HH RollUps: Apogee FEC'', count(*), count(distinct company_ID), 0 from tblChild5_{build_id}_{build}
    union
    select 5, ''DA Donor Data HH RollUps: Apogee Combined NP and FEC'', count(*), count(distinct company_ID), 0  from tblChild6_{build_id}_{build}
    union */
 /*   select 8, ''Haystaq For Selections'', count(*), 0, count(distinct Individual_ID)  from tblChild8_{build_id}_{build}
    union */
    select 9, ''Consumer Email (for matching only)'', count(*),  count(distinct company_ID), count(distinct Individual_ID)  from tblChild9_{build_id}_{build}
    union
    select 10, ''DA Consumer: Wireless - Cell Phone'', count(*), count(distinct company_ID), count(distinct Individual_ID)  from tblChild10_{build_id}_{build}
    order by 1')
to 's3://{s3-internal}{reportname2}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off;
