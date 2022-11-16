/*   Incident: 670104 (Active)  
     Lisa Fatta:  Infogroup Consumer DB (1267) - table description
     Steps: add the 9 attached tables (list below as well) for the Consumer DB so that the descriptions will be available for export on layouts?
     Created by Reeba Jacob- 03/26/2018    

--EXCLUDE_PoliticalPartyAffiliation
*/

DROP TABLE IF EXISTS {tablename6}_ToBeDropped;
CREATE TABLE {tablename6}_ToBeDropped (Code VARCHAR(1) SORTKEY PRIMARY KEY, Description VARCHAR(50));


INSERT INTO {tablename6}_ToBeDropped Values ('B','LIBERAL');
INSERT INTO {tablename6}_ToBeDropped Values ('C','CONSERVATIVE');
INSERT INTO {tablename6}_ToBeDropped Values ('D','DEMOCRATIC');
INSERT INTO {tablename6}_ToBeDropped Values ('G','GREEN');
INSERT INTO {tablename6}_ToBeDropped Values ('I','INDEPENDENT');
INSERT INTO {tablename6}_ToBeDropped Values ('L','LIBERTARIAN');
INSERT INTO {tablename6}_ToBeDropped Values ('M','REFORM');
INSERT INTO {tablename6}_ToBeDropped Values ('N','NON-DECLARED');
INSERT INTO {tablename6}_ToBeDropped Values ('O','OTHER');
INSERT INTO {tablename6}_ToBeDropped Values ('P','INDEPENDENCE');
INSERT INTO {tablename6}_ToBeDropped Values ('R','REPUBLICAN');
INSERT INTO {tablename6}_ToBeDropped Values ('U','PARTY UNKNOWN');

--SF 05.03.2019  added to ensure all descriptions are in proper case per ticket 774425
DROP TABLE IF EXISTS {tablename6}; 
SELECT Code, INITCAP(Description) as Description
INTO {tablename6}
FROM {tablename6}_ToBeDropped; 

