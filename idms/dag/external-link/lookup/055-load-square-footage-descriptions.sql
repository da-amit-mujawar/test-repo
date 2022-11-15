/*
Ticket # # 637748 

Create the table in IQ to use it as a view in Execureach (1150)

--Reju M 2019.03.15
*/


DROP TABLE IF EXISTS {tablename10};
CREATE TABLE   {tablename10} (cCode char(1) SORTKEY PRIMARY KEY, cDescription varchar(20));
INSERT INTO  {tablename10} values ('1','1 - 1,499');
INSERT INTO  {tablename10} values ('2','1,500 - 2,499');
INSERT INTO  {tablename10} values ('3','2,500 - 4,999');
INSERT INTO  {tablename10} values ('4','5,000 - 9,999');
INSERT INTO  {tablename10} values ('5','10,000 - 19,999');
INSERT INTO  {tablename10} values ('6','20,000 - 39,999');
INSERT INTO  {tablename10} values ('7','40,000 - 99,999');
INSERT INTO  {tablename10} values ('8','100,000+');










