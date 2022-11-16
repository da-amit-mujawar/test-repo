
--update topic with company matchcode

DROP TABLE IF EXISTS {table_topic_mc};

create table {table_topic_mc}
(
  account_link_id varchar(100) primary key,
  topic varchar(100),
  score varchar(50),
  company_mc varchar(15)
);

insert into {table_topic_mc}

select a.*,b.IDMS_cMatchCode
from {table_topic} a
join {table_company} b
on upper(a.account_link_id)=upper(b.account_link_id);

insert into {table_job_stats}
select 'Load Topic With MC Table',count(*),getdate() from {table_topic_mc};


--apply main and sub category
DROP TABLE IF EXISTS {table_topic_coded};

create table {table_topic_coded}
(
  account_link_id varchar(50),
  topic varchar(100),
  score varchar(5),
  company_mc varchar(15),
  main_cat varchar(500),
  sub_cat varchar(500),
  main_cat_code varchar(5),
  sub_cat_code varchar(5),
  topic_code varchar(5)
);


insert into {table_topic_coded}

select a.account_link_id,a.topic,substring(a.score,1,5) as score,a.company_mc,b.main_cat,b.sub_cat,b.main_cat_code,b.sub_cat_code,b.topic_code
from {table_topic_mc} a
left outer join {table_taxonomy} b
on upper(a.topic)=upper(b.topic);
--1 hr 42 m

insert into {table_job_stats}
select 'Load Topic With MC/SUB/MAIN Table',count(*),getdate() from {table_topic_coded};


--create final table
DROP TABLE IF EXISTS {table_topic_final};

CREATE TABLE {table_topic_final}
(
	company_mc VARCHAR(15) ENCODE zstd PRIMARY KEY DISTKEY SORTKEY,
	,main_cat_code VARCHAR(5) ENCODE zstd
	,sub_cat_code VARCHAR(5) ENCODE az64
	,topic VARCHAR(100)  ENCODE zstd
);


insert into {table_topic_final}

SELECT company_MC, main_cat_code, sub_cat_code,topic FROM {table_topic_coded} where company_mc is not null GROUP BY Company_MC, main_cat_code, sub_cat_code,Topic;
--2h 12m 49s


insert into {table_job_stats}
select 'Load Topic With MC Table After Drops',count(*),getdate() from {table_topic_final};

