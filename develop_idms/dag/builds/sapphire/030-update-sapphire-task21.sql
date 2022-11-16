/*
sharepoint#425:  Phone Multibuyer Added Into Sapphire
*/

DROP TABLE IF EXISTS SapphirePhoneCount_ToBeDropped;
Select Phone, count(Distinct ListID) as ncount
into SapphirePhoneCount_ToBeDropped
from {sapphire-tbl-ctas1}
where  ( Ltrim(Rtrim(Phone)) <>''  AND Phone is not null)
group by Phone;

--cleanup
DELETE
--Select Top 500 *
From SapphirePhoneCount_ToBeDropped
where Phone ='9999999999' OR LEN(Phone) <10  OR Phone Like '0%';

ALTER TABLE {sapphire-tbl-ctas1} ADD PhoneMultibuyerCount int;

Update {sapphire-tbl-ctas1}
  SET PhoneMultibuyerCount =B.ncount
--Select count(*)
From {sapphire-tbl-ctas1} A
inner join SapphirePhoneCount_ToBeDropped B on A.Phone =B.Phone;

Update {sapphire-tbl-ctas1}
SET PhoneMultibuyerCount = 0
where PhoneMultibuyerCount is null;

DROP TABLE IF EXISTS SapphirePhoneCount_ToBeDropped;

--TopLevelDomain handled in CTAS
--MaxPerAddressZip handled in CTAS

ALTER TABLE {sapphire-tbl-ctas1} ADD Template_Flag VARCHAR(70) DEFAULT ',X1,X2,X3,X4,X5,X6,X7,ALL,';  --'ALL' is default for all records
