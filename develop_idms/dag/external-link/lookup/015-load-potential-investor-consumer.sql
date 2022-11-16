DROP TABLE IF EXISTS {tablename2}_ToBeDropped;
CREATE TABLE {tablename2}_ToBeDropped (Code VARCHAR(2) SORTKEY PRIMARY KEY, Description VARCHAR(50));

INSERT INTO {tablename2}_ToBeDropped Values ('01','VERY AGGRESSIVE INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('02','VERY AGGRESSIVE INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('03','VERY AGGRESSIVE INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('04','AGGRESSIVE INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('05','AGGRESSIVE INVESTOR'); 						
INSERT INTO {tablename2}_ToBeDropped Values ('06','AGGRESSIVE INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('07','LIKELY INVESTOR');					
INSERT INTO {tablename2}_ToBeDropped Values ('08','LIKELY INVESTOR');							
INSERT INTO {tablename2}_ToBeDropped Values ('09','LIKELY INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('10','LIKELY INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('11','LIKELY INVESTOR');							
INSERT INTO {tablename2}_ToBeDropped Values ('12','LIKELY INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('13','LIKELY INVESTOR');							
INSERT INTO {tablename2}_ToBeDropped Values ('14','LIKELY INVESTOR');			
INSERT INTO {tablename2}_ToBeDropped Values ('15','UNLIKELY INVESTOR');							
INSERT INTO {tablename2}_ToBeDropped Values ('16','UNLIKELY INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('17','UNLIKELY INVESTOR');							
INSERT INTO {tablename2}_ToBeDropped Values ('18','UNLIKELY INVESTOR');						
INSERT INTO {tablename2}_ToBeDropped Values ('19','UNLIKELY INVESTOR');							
INSERT INTO {tablename2}_ToBeDropped Values ('20','UNLIKELY INVESTOR');							

--SF 05.03.2019  added to ensure all descriptions are in proper case per ticket 774425
DROP TABLE IF EXISTS {tablename2}; 
SELECT 	Code, INITCAP(Description) as Description
INTO 	{tablename2}
FROM 	{tablename2}_ToBeDropped; 				
					
						

