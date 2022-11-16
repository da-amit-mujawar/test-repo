/*   Incident: 670104 (Active)  
     Lisa Fatta:  Infogroup Consumer DB (1267) - table description
     Steps: add the 9 attached tables (list below as well) for the Consumer DB so that the descriptions will be available for export on layouts?
     Created by Reeba Jacob- 03/26/2018    
--EXCLUDE_WealthFinder
*/

DROP TABLE IF EXISTS {tablename4};
CREATE TABLE {tablename4} (Code VARCHAR(1) SORTKEY PRIMARY KEY, Description VARCHAR(50));

--SF 05.14.2019  modified per ticket 777003 to update value.
INSERT INTO {tablename4} Values ('A','$13,693,440 +');						
INSERT INTO {tablename4} Values ('B','$8,693,520 - $13,693,439');					
INSERT INTO {tablename4} Values ('C','$3,693,600 - $8,693,519');						
INSERT INTO {tablename4} Values ('D','$3,218,667 - $3,693,599');						
INSERT INTO {tablename4} Values ('E','$2,743,733 - $3,218,666');						
INSERT INTO {tablename4} Values ('F','$1,186,300 - $2,743,732');						
INSERT INTO {tablename4} Values ('G','$739,000 - $1,186,299');					
INSERT INTO {tablename4} Values ('H','$554,050 - $738,999');						
INSERT INTO {tablename4} Values ('I','$369,100 - $554,049');						
INSERT INTO {tablename4} Values ('J','$295,000 - $369,099');						
INSERT INTO {tablename4} Values ('K','$220,900 - $294,999');						
INSERT INTO {tablename4} Values ('L','$173,850 - $220,899');						
INSERT INTO {tablename4} Values ('M','$126,800 - $173,849');						
INSERT INTO {tablename4} Values ('N','$97,300 - $126,799'); 					
INSERT INTO {tablename4} Values ('O','$71,501 - $97,299');					
INSERT INTO {tablename4} Values ('P','$51,303 - $71,500');					
INSERT INTO {tablename4} Values ('Q','$20,703 - $51,302');					
INSERT INTO {tablename4} Values ('R','$5,700 - $20,702');					
INSERT INTO {tablename4} Values ('S','$550 - $5,699');					
INSERT INTO {tablename4} Values ('T','$0 - $549');	


/*
INSERT INTO {tablename4} Values ('A','$2,729,000 +');							
INSERT INTO {tablename4} Values ('B','$2,551,000 - $2,728,999');							
INSERT INTO {tablename4} Values ('C','$2,385,000 - $2,550,999');							
INSERT INTO {tablename4} Values ('D','$2,229,000 - $2,384,999');							
INSERT INTO {tablename4} Values ('E','$2,084,000 - $2,228,999');							
INSERT INTO {tablename4} Values ('F','$1,488,000 - $2,083,999');							
INSERT INTO {tablename4} Values ('G','$1,062,000 - $1,487,999');							
INSERT INTO {tablename4} Values ('H','$757,000 - $1,061,999');							
INSERT INTO {tablename4} Values ('I','$540,000 - $756,999');							
INSERT INTO {tablename4} Values ('J','$385,000 - $539,999');							
INSERT INTO {tablename4} Values ('K','$274,000 - $384,999');							
INSERT INTO {tablename4} Values ('L','$195,000 - $273,999');							
INSERT INTO {tablename4} Values ('M','$138,000 - $194,999');							
INSERT INTO {tablename4} Values ('N','$98,000 - $137,999');							
INSERT INTO {tablename4} Values ('O','$69,000 - $97,999');							
INSERT INTO {tablename4} Values ('P','$48,000 - $68,999');							
INSERT INTO {tablename4} Values ('Q','$23,000 - $47,999');							
INSERT INTO {tablename4} Values ('R','$10,000 - $22,999');							
INSERT INTO {tablename4} Values ('S','$4,000 - $9,999');							
INSERT INTO {tablename4} Values ('T','$0 - $3,999');							
*/




