

/*   Incident: 670104 (Active)  
     Lisa Fatta:  Infogroup Consumer DB (1267) - table description
     Steps: add the 9 attached tables (list below as well) for the Consumer DB so that the descriptions will be available for export on layouts?
     Created by Reeba Jacob- 03/26/2018   

--EXCLUDE_InfopersonaSupercluster
 */
 
DROP TABLE IF EXISTS {tablename7};
CREATE TABLE {tablename7} (Code VARCHAR(1) SORTKEY PRIMARY KEY, Description VARCHAR(50));


INSERT INTO {tablename7} Values ('1','Upper Crust');
INSERT INTO {tablename7} Values ('2','High Fidelity');
INSERT INTO {tablename7} Values ('3','Net Worth & Networks');
INSERT INTO {tablename7} Values ('4','Picket Fences');
INSERT INTO {tablename7} Values ('5','Maintaining a Balance');
INSERT INTO {tablename7} Values ('6','Ways & Means');
INSERT INTO {tablename7} Values ('7','Golden Years');
INSERT INTO {tablename7} Values ('8','Debt Builders');
INSERT INTO {tablename7} Values ('9','Hardscrabbles');




