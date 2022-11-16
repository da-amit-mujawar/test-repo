/*
Copied from Infogroup business 992 code.
Added cShortDescription as per Ticket 809910

Reju Mathew 2019.10.23
*/


drop table if exists {tablename13} ;
Create table {tablename13} (cCode char(3) SORTKEY PRIMARY KEY, cDescription varchar(35), cShortDescription varchar(35));
--Truncate table {tablename13}; 


INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('1', 'Owner','Owner');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('2', 'President','President');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('3', 'Manager','Manager');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('4', 'Executive Director','Exec Director');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('5', 'Principal','Principal');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('6', 'Publisher/Editor','Publisher/Editor');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('7', 'Administrator','Administrator');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('8', 'Religious Leader','Religious Dir');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('9', 'Partner','Partner');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('A', 'Chairman','Chairman');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('C', 'CEO','CEO');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('D', 'Board Member','Board Member');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('E', 'COO','COO');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('F', 'CFO','CFO');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('G', 'Treasurer','Treasurer');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('H', 'Controller','Controller');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('I', 'Executive Vice President','Executive VP');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('J', 'Senior Vice President','Senior VP');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('K', 'Vice President','Vice President');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('L', 'Administration Executive','Admin Exec');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('M', 'Corporate Communications Executive','Corp Comms');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('N', 'IT Executive','IT Executive');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('O', 'Finance Executive','Finance Executive');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('P', 'Human Resources Executive','HR Executive');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Q', 'Telecommunications Executive','Telecom Exec');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('R', 'Marketing Executive','Marketing Exec');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('S', 'Operations Executive','Operation Exec');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('T', 'Sales Executive','Sales Exec');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('V', 'Legal','Legal');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('W', 'Executive Officer','Exec Officer');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('X', 'Manufacturing Executive','Mfg Exec');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Y', 'Purchasing Executive','Purchase Exec');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z1', 'IT','IT');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z10', 'International','International');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z11', 'Manufacturing','Manufacturing');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z12', 'Educator','Educator');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z13', 'Engineering/Technical','Engr/Tech');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z14', 'General Manager','General Mgr');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z15', 'Office Manager','Office Manager');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z16', 'CIO/CTO','CIO/CTO');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z17', 'Operations','Operations');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z18', 'Marketing','Marketing');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z19', 'Other','Other');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z2', 'Finance','Finance');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z20', 'Human Resources','HR');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z21', 'Site Manager','Site Manager');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z22', 'Regional Manager','Regional Mgr');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z3', 'Chief Administrative Officer','CAO');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z4', 'Chief Marketing Office','CMO');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z5', 'Business Development','Business Dev');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z6', 'Director','Director');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z7', 'Executive','Executive');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z8', 'Facilities','Facilities');
INSERT INTO {tablename13} (cCode, cDescription, cShortDescription)  Values ('Z9', 'Sales','Sales');



