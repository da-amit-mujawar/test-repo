/*   Incident: 670104 (Active)  
     Lisa Fatta:  Infogroup Consumer DB (1267) - table description
     Steps: add the 9 attached tables (list below as well) for the 
     Infogroup Consumer DB (1267) so that the descriptions will be available for export on layouts?
     Created by Reeba Jacob- 03/26/2018   

--EXCLUDE_AircraftTypeCode
 */
 
DROP TABLE IF EXISTS {tablename1};
CREATE TABLE {tablename1} (Code VARCHAR(1) SORTKEY PRIMARY KEY, Description VARCHAR(50));

INSERT INTO {tablename1} Values ('B','Balloon');
INSERT INTO {tablename1} Values ('G','Glider');
INSERT INTO {tablename1} Values ('J','Fixed Wing Engine Jet');
INSERT INTO {tablename1} Values ('P','Fixed Wing Engine Prop');
INSERT INTO {tablename1} Values ('R','Rotary');





