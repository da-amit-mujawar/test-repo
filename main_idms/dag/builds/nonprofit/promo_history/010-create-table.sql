-- running for both the databases using 2 tasks (different source tables)
-- database_id for apogge: 74
-- database_id for donorbase: 1438

CREATE TABLE IF NOT EXISTS datascience.promo_history_{databaseid_apogee} (
	Individual_ID varchar(25) ENCODE ZSTD,
	Family_ID varchar(25) ENCODE ZSTD,
	ListID int ENCODE AZ64,
	AccountNo varchar(100)  ENCODE ZSTD,
    MailDate varchar(10)  ENCODE ZSTD,
	Detail_DonationDate varchar(8)  ENCODE ZSTD,
	Detail_DonationDollar int  ENCODE AZ64,
	CampaignName varchar(100)  ENCODE ZSTD,
	CampaignAudiences1 varchar(25)  ENCODE ZSTD,
	CampaignAudiences2 varchar(25)  ENCODE ZSTD,
	KeyCode varchar(100)  ENCODE ZSTD,
	PackageCode varchar(25)  ENCODE ZSTD,
	KeyCodeDescription varchar(100)  ENCODE ZSTD,
	PackageCodeDescription varchar(100)  ENCODE ZSTD,
	ListProvider varchar(25)  ENCODE ZSTD,
	ListType varchar(1)  ENCODE ZSTD,
	OwnerID int ENCODE AZ64,
    ID bigint IDENTITY ENCODE DELTA
);

CREATE TABLE IF NOT EXISTS datascience.promo_history_{databaseid_donorbase}(LIKE datascience.promo_history_{databaseid_apogee});

CREATE TABLE IF NOT EXISTS datascience.PROMO_HISTORY_STAGING (
    id bigint ENCODE DELTA,
	ListID int ENCODE AZ64,
	AccountNo varchar(100)  ENCODE ZSTD,
	FullName varchar(50)  ENCODE ZSTD,
	FirstName varchar(20)  ENCODE ZSTD,
	LastName varchar(20)  ENCODE ZSTD,
	AddressLine1 varchar(250)  ENCODE ZSTD,
	AddressLine2 varchar(150)  ENCODE ZSTD,
	City varchar(100)  ENCODE ZSTD,
	State varchar(10)  ENCODE ZSTD,
	ZIPFULL varchar(15)  ENCODE ZSTD,
	ZIP varchar(10)  ENCODE ZSTD,
	ZIP4 varchar(4)  ENCODE ZSTD,
	Phone varchar(20)  ENCODE ZSTD,
	EmailAddress varchar(200)  ENCODE ZSTD,
	CampaignName varchar(100)  ENCODE ZSTD,
	CampaignAudiences1 varchar(25)  ENCODE ZSTD,
	CampaignAudiences2 varchar(25)  ENCODE ZSTD,
	Detail_DonationDollar int  ENCODE AZ64,
	Detail_DonationDate varchar(8)  ENCODE ZSTD,
	Detail_PaymentMethod varchar(1)  ENCODE ZSTD,
	Detail_DonationChannel varchar(1)  ENCODE ZSTD,
	MailDate varchar(10)  ENCODE ZSTD,
	KeyCode varchar(100)  ENCODE ZSTD,
	KeyCodeDescription varchar(100)  ENCODE ZSTD,
	PackageCode varchar(25)  ENCODE ZSTD,
	PackageCodeDescription varchar(100)  ENCODE ZSTD,
	ListProvider varchar(25)  ENCODE ZSTD,
	Individual_ID varchar(25) ENCODE ZSTD,
	Family_ID varchar(25) ENCODE ZSTD,
	IndividualMC varchar(25)  ENCODE ZSTD,
	CompanyMC varchar(25)  ENCODE ZSTD,
	Mailabilityscore varchar(2)  ENCODE ZSTD,
	DeceasedFlag varchar(1) NULL ENCODE ZSTD,
	DropFlag varchar(1)  ENCODE ZSTD,
	ListType varchar(1)  ENCODE ZSTD,
	OwnerID int ENCODE AZ64
);
