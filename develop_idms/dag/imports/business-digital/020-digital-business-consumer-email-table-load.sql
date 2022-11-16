--consumer email
copy {table_consumer_email_raw}
from 's3://{s3-cdbus-path}{s3-consumer-key11}'
iam_role '{iam}'
FORMAT AS PARQUET;

drop table if exists {table_consumer_email};

create table {table_consumer_email} as
select cast(seq as varchar(30)) id,
Acquisition_Date, VendorID2, Usage_Indicator, Firstname, cast('' as varchar(15)) as d_firstname  , MiddleInitial, Lastname, cast('' as varchar(20)) as d_lastname , Gender, Housenumber, Streetpredirectional, Streetname, cast('' as varchar(28)) as d_streetname  ,Streetsuffix, Streetpostdirectional, UnitType, Unitnumber, City, cast('' as varchar(28)) as d_city , State, Zipcode, Zipfour, Move_Ind, Emailaddress, Responderdate, Suppression_type, FamilyId, IndividualId, Match_Ind, Mailconfidence, Recordtype, Alsincome, Alsagecode, Alslengthofresidence, Alspurchasingpowerincome, Alshomevalue, Alswealthfinder, psfducode, p10ducode, pownrocccode, IP_Address, Optin_Date, Url, StateCode, CensusCountyCode, CensusTract, CensusBlockGroup, MatchCode, Editedaddress, EmailDatabase_Extent, OldMasterIndicator, PrioritySourceCode, RoadRunner_Flag, Source_Counter, Source_Code_Indicator1, Source_Code_Indicator2, Source_Code_Indicator3, Latitude, Longitude, ContactID vendorcustomerid, GST_SourceCode_Indicator, GST_Source_Counter, LemsMatchCode, Reject_Reason, DMA_Code, BVT_Email_Status, BVT_Refresh_Date, IPST_Validity_Score, IPST_Status_Code, IPST_Refresh_Date, Category, Email_Clickthru_Date, Email_Open_Date, Domain, Top_Level_Domain, DOB_Individual, Home_Owner, Best_Date, Emaildb_Flag, CountryCode
    from {table_consumer_email_raw};











