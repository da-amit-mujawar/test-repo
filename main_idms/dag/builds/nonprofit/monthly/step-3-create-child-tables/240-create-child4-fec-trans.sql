/*
 Create Apogee Child4 Table FEC Transactions.
 Source is Flat Files exported from On-prem SQL Server, DataSynced into S3.
 We need to build Airflow DAG to export this directly from Athena in the same format. TBD
 Until the DAG is built, Caroline will have continue to run this FEC Export from SQL to flat file step in VC.
 
 Update Schedule: Monthly

 --20221003 CB Remove FEC from build
*/

-- create child4;

drop table if exists no_such_table;  --exclude_nonprofit_transactions_fec_{dbid};
/*
create table exclude_nonprofit_transactions_fec_{dbid}
(
    sourcelistid           integer           encode az64,
    individual_id          varchar(25)       encode zstd,
    company_id             varchar(25)       distkey sortkey,
    accountno              varchar(100)      encode zstd,
    listcategory01         char(2)           encode zstd,
    listcategory02         char(2)           encode zstd,
    listcategory03         char(2)           encode zstd,
    listcategory04         char(2)           encode zstd,
    listcategory05         char(2)           encode zstd,
    party_name             varchar(20)       encode zstd,
    isblueflag             varchar(20)       encode zstd,
    committeeid            varchar(20)       encode zstd,
    raw_field04            varchar(10)       encode zstd,
    raw_field05            varchar(10)       encode zstd,
    raw_field06            varchar(10)       encode zstd,
    raw_field07            varchar(10)       encode zstd,
    raw_field08            varchar(10)       encode zstd,
    raw_field09            varchar(10)       encode zstd,
    partynamecode          varchar(20)       encode zstd,
    detail_donationdollar  integer           encode az64,
    detail_donationdate    varchar(8)        encode zstd,
    detail_paymentmethod   char(1)           encode zstd,
    detail_donationchannel char(1)           encode zstd
);

/*
COPY TABLE exclude_nonprofit_transactions_fec_{dbid}
need to load from athena directly
*/

insert into exclude_nonprofit_transactions_fec_{dbid}
    select
            cast(sourcelistid as integer)       as sourcelistid,
            left(trim(individual_id), 25)       as individual_id,
            left(trim(company_id), 20)          as company_id,
            left(trim(accountno), 100)          as accountno,
            left(trim(listcategory01), 2)       as listcategory01,
            left(trim(listcategory02), 2)       as listcategory02,
            left(trim(listcategory03), 2)       as listcategory03,
            left(trim(listcategory04), 2)       as listcategory04,
            left(trim(listcategory05), 2)       as listcategory05,
            left(trim(party_name), 20)          as party_name,
            left(trim(isblueflag), 20)          as isblueflag,
            left(trim(committeeid), 20)         as committeeid,
            left(trim(raw_field04), 10)         as raw_field04,
            left(trim(raw_field05), 10)         as raw_field05,
            left(trim(raw_field06), 10)         as raw_field06,
            left(trim(raw_field07), 10)         as raw_field07,
            left(trim(raw_field08), 10)         as raw_field08,
            left(trim(raw_field09), 10)         as raw_field09,
            left(trim(raw_field10), 10)         as partynamecode,
            nvl(detail_donationdollar, 0)       as detail_donationdollar,
            left(trim(detail_donationdate), 8)  as detail_donationdate
     from exclude_trans_fec;

*/

