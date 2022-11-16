BEGIN;
alter table {maintable_name} add cdb_date_recency_cat_ind int ENCODE lzo;
alter table {maintable_name} add cdb_date_recency_pub_ind int ENCODE lzo;
alter table {maintable_name} add cdb_date_recency_np_ind int ENCODE lzo;

alter table {maintable_name} add cdb_date_recency_cat_hh int ENCODE lzo;
alter table {maintable_name} add cdb_date_recency_pub_hh int ENCODE lzo;
alter table {maintable_name} add cdb_date_recency_np_hh int ENCODE lzo;

alter table {maintable_name} add cdb_date_recency_total_ind int ENCODE lzo;
alter table {maintable_name} add cdb_date_recency_total_hh int ENCODE lzo;
END;

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	individual_mc varchar(27),
    maxdate  varchar(6),
    recency int
   );
END;


BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc, maxdate)
select individual_mc, max(consumer_db_orderdate_cat) as maxdate
from {maintable_name} 
where consumer_db_list_category ='2'
and consumer_db_orderdate_cat is not null and consumer_db_orderdate_cat not in ('',' ','000000')
group by individual_mc
order by individual_mc;
END;

BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate()); 
END;
/*
update tblmain
*/
BEGIN;
update {maintable_name}
set cdb_date_recency_cat_ind = recency
from {maintable_name} a
inner join  prospector_tempcalculations_tobedropped b on a.individual_mc = b.individual_mc 
  and (a.consumer_db_orderdate_cat is not null and a.consumer_db_orderdate_cat not in ('',' ','000000'));
END;

------------------------------
-- cdb_date_recency_pub_ind
------------------------------

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	individual_mc varchar(27),
    maxdate  varchar(6),
    recency int
   );
END;
/*
load tempcalc
*/
BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc, maxdate)
select individual_mc, max(consumer_db_orderdate_pub) as maxdate
from {maintable_name} 
where consumer_db_list_category in ('1','4','6')
and consumer_db_orderdate_pub is not null and consumer_db_orderdate_pub not in ('',' ','000000')
group by individual_mc
order by individual_mc;
END;

/*
calculate recency
*/
BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate());
END;
/*
update tblmain
*/
BEGIN;
update {maintable_name}
set cdb_date_recency_pub_ind = recency
from {maintable_name} a
inner join  prospector_tempcalculations_tobedropped b on a.individual_mc = b.individual_mc 
  and (a.consumer_db_orderdate_pub is not null and a.consumer_db_orderdate_pub not in ('',' ','000000'));
END;


------------------------------
-- cdb_date_recency_np_ind
------------------------------

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	individual_mc varchar(27),
    maxdate  varchar(6),
    recency int
   );
END;

/*
load tempcalc
*/

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc, maxdate)
select individual_mc, max(consumer_db_orderdate_np) as maxdate
from {maintable_name}
where consumer_db_list_category in ('3','5')
and consumer_db_orderdate_np is not null and consumer_db_orderdate_np not in ('',' ','000000')
group by individual_mc
order by individual_mc;
END;

/*
calculate recency
*/
BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate()); 
END;

/*
update tblmain
*/

BEGIN;
update {maintable_name}
set cdb_date_recency_np_ind = recency
from {maintable_name} a
inner join  prospector_tempcalculations_tobedropped b on a.individual_mc = b.individual_mc 
  and (a.consumer_db_orderdate_np is not null and a.consumer_db_orderdate_np not in ('',' ','000000'));
END;

/*
--=================================================================================================================================
--  household fields
--=================================================================================================================================

-- hh fields based on lems 

------------------------------
-- cdb_date_recency_cat_hh
------------------------------
*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	lems varchar(18),    
	maxdate  varchar(6),
    recency int
   );
END;

/*
load tempcalc
*/
BEGIN;
insert into prospector_tempcalculations_tobedropped (lems, maxdate)
select lems, max(consumer_db_orderdate_cat) as maxdate
from {maintable_name} 
where consumer_db_list_category ='2'
and consumer_db_orderdate_cat is not null and consumer_db_orderdate_cat not in ('',' ','000000')
group by lems
order by lems;
END;

/*
calculate recency
*/
BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate()); 
END;
/*
update tblmain
*/
BEGIN;
update {maintable_name}
set cdb_date_recency_cat_hh = recency
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on a.lems = b.lems 
  and (a.consumer_db_orderdate_cat is not null and a.consumer_db_orderdate_cat not in ('',' ','000000'));
END;

------------------------------
-- cdb_date_recency_pub_hh
------------------------------

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	lems varchar(18),    
	maxdate  varchar(6),
    recency int
   );
END;

/*
load tempcalc
*/
BEGIN;
insert into prospector_tempcalculations_tobedropped (lems, maxdate)
select lems, max(consumer_db_orderdate_pub) as maxdate
from {maintable_name} 
where consumer_db_list_category in ('1','4','6')
and consumer_db_orderdate_pub is not null and consumer_db_orderdate_pub not in ('',' ','000000')
group by lems
order by lems;
END;

/*
calculate recency
*/
BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate()); 
END;

/*
update tblmain
*/
BEGIN;
update {maintable_name}
set cdb_date_recency_pub_hh = recency
from {maintable_name} a
inner join  prospector_tempcalculations_tobedropped b on a.lems = b.lems 
  and (a.consumer_db_orderdate_pub is not null and a.consumer_db_orderdate_pub not in ('',' ','000000'));
END;

/*
------------------------------
-- cdb_date_recency_np_hh
------------------------------
*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	lems varchar(18),    
	maxdate  varchar(6),
    recency int
   );
END;

/*
load tempcalc
*/

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems, maxdate)
select lems, max(consumer_db_orderdate_np) as maxdate
from {maintable_name} 
where consumer_db_list_category in ('3','5')
and consumer_db_orderdate_np is not null and consumer_db_orderdate_np not in ('',' ','000000')
group by lems
order by lems;
END;

/*
--calculate recency
*/
BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate()); 
END;

/*
update tblmain
*/
BEGIN;
update {maintable_name}
set cdb_date_recency_np_hh = recency
from {maintable_name} a
inner join  prospector_tempcalculations_tobedropped b on a.lems = b.lems 
  and (a.consumer_db_orderdate_np is not null and a.consumer_db_orderdate_np not in ('',' ','000000'));
END;
------------------------------
-- cdb_date_recency_total_ind
------------------------------

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	individual_mc varchar(27),
    maxdate  varchar(6),
    recency int
   );
END;
/*
load tempcalc
*/
BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc, maxdate)
select individual_mc, max(cdb_orderdate_total_ind) as maxdate
from {maintable_name} 
where cdb_orderdate_total_ind is not null and cdb_orderdate_total_ind not in ('',' ','000000')
group by individual_mc
order by individual_mc;
END;

/*
calculate recency
*/
BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate()); 
END;

/*
update tblmain
*/
BEGIN;
update {maintable_name}
set cdb_date_recency_total_ind = recency
from {maintable_name} a
inner join  prospector_tempcalculations_tobedropped b on a.individual_mc = b.individual_mc 
  and (a.cdb_orderdate_total_ind is not null and a.cdb_orderdate_total_ind not in ('',' ','000000'));
END;
/*
------------------------------
-- cdb_date_recency_total_hh
------------------------------
*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	lems varchar(18),    
	maxdate  varchar(6),
    recency int
   );
END;

/*
load tempcalc
*/
BEGIN;
insert into prospector_tempcalculations_tobedropped (lems, maxdate)
select lems, max(cdb_orderdate_total_hh) as maxdate
from {maintable_name} 
where cdb_orderdate_total_hh is not null and cdb_orderdate_total_hh not in ('',' ','000000')
group by lems
order by lems;
END;
/*
calculate recency
*/
BEGIN;
update prospector_tempcalculations_tobedropped 
   set recency = datediff(month,cast(maxdate+'15' as timestamp),getdate()); 
END;

/*
update tblmain
*/
BEGIN;
update {maintable_name}
set cdb_date_recency_total_hh = recency
from {maintable_name} a
inner join  prospector_tempcalculations_tobedropped b on a.lems = b.lems 
  and (a.cdb_orderdate_total_hh is not null and a.cdb_orderdate_total_hh not in ('',' ','000000'));
END;
