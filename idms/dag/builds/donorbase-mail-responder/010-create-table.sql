CREATE TABLE IF NOT EXISTS Donorbase_Mail_Responder (
	TableID bigint ENCODE DELTA,
	ListID int  ENCODE AZ64 DISTKEY SORTKEY,
	AccountNo varchar(100)  ENCODE ZSTD,
	FullName varchar(50)  ENCODE ZSTD,
	FirstName varchar(20)  ENCODE ZSTD,
	LastName varchar(20)  ENCODE ZSTD,
	Title varchar(50)  ENCODE ZSTD,
	Company varchar(50)  ENCODE ZSTD,
	AddressLine1 varchar(250)  ENCODE ZSTD,
	AddressLine2 varchar(150)  ENCODE ZSTD,
	City varchar(100)  ENCODE ZSTD,
	State varchar(10)  ENCODE ZSTD,
	ZIP varchar(10)  ENCODE ZSTD,
	ZIPFULL varchar(15)  ENCODE ZSTD,
	ZIP4 varchar(4)  ENCODE ZSTD,
	SCF varchar(3)  ENCODE ZSTD,
	Phone varchar(20)  ENCODE ZSTD,
	EmailAddress varchar(200)  ENCODE ZSTD,
	Gender varchar(1)  ENCODE ZSTD,
	ListType varchar(1)  ENCODE ZSTD,
	ProductCode varchar(2)  ENCODE ZSTD,
	BatchDate varchar(7)  ENCODE ZSTD,
	Individual_ID varchar(25)  ENCODE ZSTD,
	Company_ID varchar(25)  ENCODE ZSTD,
	DropFlag varchar(1)  ENCODE ZSTD,
	PermissionType varchar(1)  ENCODE ZSTD,
	ListCategory01 varchar(2)  ENCODE ZSTD,
	ListCategory02 varchar(2)  ENCODE ZSTD,
	ListCategory03 varchar(2)  ENCODE ZSTD,
	ListCategory04 varchar(2)  ENCODE ZSTD,
	ListCategory05 varchar(2)  ENCODE ZSTD,
	List_VolunteerInd varchar(1)  ENCODE ZSTD,
	Detail_DonationDollar int  ENCODE AZ64,
	Detail_DonationDate varchar(8)  ENCODE ZSTD,
	Detail_PaymentMethod varchar(1)  ENCODE ZSTD,
	Detail_DonationChannel varchar(1)  ENCODE ZSTD,
	Deceased varchar(20)  ENCODE ZSTD,
	DoNotMail varchar(20)  ENCODE ZSTD,
	DoNotCall varchar(20)  ENCODE ZSTD,
	RawField01 varchar(50)  ENCODE ZSTD,
	RawField02 varchar(50)  ENCODE ZSTD,
	RawField03 varchar(50)  ENCODE ZSTD,
	RawField04 varchar(50)  ENCODE ZSTD,
	RawField05 varchar(50)  ENCODE ZSTD,
	RawField06 varchar(50)  ENCODE ZSTD,
	RawField07 varchar(50)  ENCODE ZSTD,
	RawField08 varchar(50)  ENCODE ZSTD,
	RawField09 varchar(50)  ENCODE ZSTD,
	RawField10 varchar(50)  ENCODE ZSTD,
	RawField11 varchar(50)  ENCODE ZSTD,
	RawField12 varchar(50)  ENCODE ZSTD,
	RawField13 varchar(50)  ENCODE ZSTD,
	RawField14 varchar(50)  ENCODE ZSTD,
	RawField15 varchar(50)  ENCODE ZSTD,
	IndividualMC varchar(25)  ENCODE ZSTD,
	CompanyMC varchar(25)  ENCODE ZSTD,
	MailDate varchar(10)  ENCODE ZSTD,
	SourceCode1 varchar(50)  ENCODE ZSTD,
	SourceCode2 varchar(50)  ENCODE ZSTD,
	SourceCode3 varchar(50)  ENCODE ZSTD,
	SourceCode4 varchar(50)  ENCODE ZSTD,
	SourceCode5 varchar(50)  ENCODE ZSTD,
	Mailabilityscore varchar(2)  ENCODE ZSTD,
	DeceasedFlag varchar(1) NULL ENCODE ZSTD,
	ID bigint IDENTITY ENCODE DELTA
);
