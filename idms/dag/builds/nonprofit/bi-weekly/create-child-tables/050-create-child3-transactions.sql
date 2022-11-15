DROP TABLE IF EXISTS exclude_nonprofit_transactions_{dbid};

CREATE TABLE exclude_nonprofit_transactions_{dbid}
(
TableID bigint ENCODE ZSTD,
SourceListID int ENCODE ZSTD,
Individual_ID varchar(25) DISTKEY SORTKEY,
Company_ID varchar(25) ENCODE ZSTD,
AccountNo varchar(100) ENCODE ZSTD,
ListCategory01 varchar(2) ENCODE ZSTD,
List_TotalNumberDonations int ENCODE AZ64,
ListCategory02 varchar(2) ENCODE ZSTD,
ListCategory03 varchar(2) ENCODE ZSTD,
ListCategory04 varchar(2) ENCODE ZSTD,
ListCategory05 varchar(5) ENCODE ZSTD,
List_TotalDollarDonations int ENCODE AZ64,
List_LastDateDonation varchar(8) ENCODE ZSTD,
List_FirstDateDonation varchar(8) ENCODE ZSTD,
List_LastPaymentMethod varchar(25) ENCODE ZSTD,
List_LastChannel varchar(1) ENCODE ZSTD,
List_LastDollarDonation int ENCODE AZ64,
List_HighestDollarDonation int ENCODE ZSTD,
List_LowestDollarDonation int ENCODE AZ64,
List_WeeksSinceLastDonation int ENCODE AZ64,
List_VolunteerInd char(1) ENCODE ZSTD,
Detail_DonationDollar int ENCODE ZSTD,  
Detail_DonationDate varchar(8) ENCODE ZSTD,
Detail_PaymentMethod char(1) ENCODE ZSTD,
Detail_DonationChannel char(1) ENCODE ZSTD
);
 

COPY exclude_nonprofit_transactions_{dbid}
FROM '{manifest_path}'
MANIFEST
IAM_ROLE '{iam}'
DELIMITER '|'
GZIP
TRUNCATECOLUMNS
ACCEPTINVCHARS
TRIMBLANKS
MAXERROR 1000;


/*
DROP TABLE IF EXISTS {child1};
ALTER TABLE DonorBase_Transactions RENAME TO {child1};

--rename rollup tables
ALTER TABLE exclude_sum_ltd_db RENAME TO tblChild2_{build_id}_{build};
ALTER TABLE exclude_sum_cat_db RENAME TO tblChild4_{build_id}_{build};
ALTER TABLE exclude_sum_avg_list_db RENAME TO tblChild5_{build_id}_{build};
ALTER TABLE exclude_sum_amt_list_db RENAME TO tblChild6_{build_id}_{build};
ALTER TABLE exclude_sum_nbr_list_db RENAME TO tblChild7_{build_id}_{build};
*/

