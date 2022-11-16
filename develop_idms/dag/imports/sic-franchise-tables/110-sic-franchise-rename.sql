-- replace old table with new
-- drop table if exists tblsiccode;
-- drop table if exists sql_tblsiccode;
drop table if exists sql_tblsicfranchisecode;
drop table if exists tblindustrycode;
drop table if exists sql_tblindustrycode;

-- rename new table for use
-- alter table tblsiccode_new rename to tblsiccode;
-- alter table sql_tblsiccode_new rename to sql_tblsiccode;
alter table sql_tblsicfranchisecode_new rename to sql_tblsicfranchisecode;
alter table tblindustrycode_new rename to tblindustrycode;
alter table sql_tblindustrycode_new rename to sql_tblindustrycode;

