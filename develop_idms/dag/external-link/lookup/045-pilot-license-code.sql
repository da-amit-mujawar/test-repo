/*   Incident: 670104 (Active)  
     Lisa Fatta:  Infogroup Consumer DB (1267) - table description
     Steps: add the 9 attached tables (list below as well) for the Consumer DB so that the descriptions will be available for export on layouts?
     Created by Reeba Jacob- 03/26/2018   

--EXCLUDE_PilotLicenseCode
 */
 
DROP TABLE IF EXISTS {tablename8};
CREATE TABLE {tablename8} (Code VARCHAR(1) SORTKEY PRIMARY KEY, Description VARCHAR(50));

INSERT INTO {tablename8} Values ('C','Commercial Pilot');
INSERT INTO {tablename8} Values ('P','Private Pilot');
INSERT INTO {tablename8} Values ('S','Student Pilot');

