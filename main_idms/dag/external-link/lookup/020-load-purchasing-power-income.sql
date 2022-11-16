

/*   Incident: 670104 (Active)  
     Lisa Fatta:  Infogroup Consumer DB (1267) - table description
     Steps: add the 9 attached tables (list below as well) for the Consumer DB so that the descriptions will be available for export on layouts?
     Created by Reeba Jacob- 03/26/2018    

--EXCLUDE_PotentialInvestorConsumer

*/

DROP TABLE IF EXISTS {tablename2};
CREATE TABLE {tablename2} (Code VARCHAR(1) SORTKEY PRIMARY KEY, Description VARCHAR(50));


INSERT INTO {tablename2} Values ('A','UNDER 20,000');						
INSERT INTO {tablename2} Values ('B','$20,000 - 29,999');							
INSERT INTO {tablename2} Values ('C','$30,000 - $39,999');						
INSERT INTO {tablename2} Values ('D','$40,000 - $49,999');						
INSERT INTO {tablename2} Values ('E','$50,000 - $59,999');							
INSERT INTO {tablename2} Values ('F','$60,000 - $69,999');							
INSERT INTO {tablename2} Values ('G','$70,000 - $79,999');							
INSERT INTO {tablename2} Values ('H','$80,000 - $89,999');							
INSERT INTO {tablename2} Values ('I','$90,000 - $99,999');						
INSERT INTO {tablename2} Values ('J','$100,000 - $124,999');							
INSERT INTO {tablename2} Values ('K','$125,000 - $149,999');							
INSERT INTO {tablename2} Values ('L','$150,000 - $174,999');						
INSERT INTO {tablename2} Values ('M','$175,000 - $199,999');							
INSERT INTO {tablename2} Values ('N','$200,000 - $249,999');							
INSERT INTO {tablename2} Values ('O','$250,000 - $299,999');							
INSERT INTO {tablename2} Values ('P','$300,000 - $399,999');							
INSERT INTO {tablename2} Values ('Q','$400,000 - $499,999');							
INSERT INTO {tablename2} Values ('R','$500,000 Plus');						
					
							


