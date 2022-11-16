BEGIN;
alter table {maintable_name} add cdb_recency_cat int ENCODE lzo;
alter table {maintable_name} add cdb_recency_pub int ENCODE lzo;
alter table {maintable_name} add cdb_recency_np  int ENCODE lzo;
END;

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	sourcelistid int,    
	maxdate  varchar(6)
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (sourcelistid,maxdate)
select sourcelistid, max(consumer_db_orderdate_cat) as maxdate
from {maintable_name}
where consumer_db_list_category ='2'
group by sourcelistid
order by sourcelistid;
END;
/*
delete the blanks(empty records)  and non numeric data. reju m
*/
BEGIN;
delete from prospector_tempcalculations_tobedropped where maxdate ='';
END;

/*
update cdb_recency_cat
*/
BEGIN;
update {maintable_name}
    set cdb_recency_cat = case when b.maxdate = a.consumer_db_orderdate_cat then 1 
							   else datediff(month,cast(a.consumer_db_orderdate_cat+'15' as timestamp),cast(b.maxdate+'15' as timestamp)) end
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on a.sourcelistid = b.sourcelistid
  --and (cdb_freq_orders_cat_hh >4 and cdb_freq_orders_cat_hh is not null) for auditing report filter
  and (a.consumer_db_orderdate_cat is not null and a.consumer_db_orderdate_cat not in ('',' ','000000'));
END;

/*
fields to be populated  for consumer_db_orderdate_pub
cdb_recency_pub
*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	sourcelistid int,    
	maxdate  varchar(6)
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (sourcelistid,maxdate)
select sourcelistid, max(consumer_db_orderdate_pub) as maxdate
from {maintable_name} 
where consumer_db_list_category in ('1','4','6')
group by sourcelistid
order by sourcelistid;
END;


/*
delete the blanks(empty records)
*/
BEGIN;
delete from prospector_tempcalculations_tobedropped where maxdate ='';
END;

/*
update tblmain
*/
BEGIN;
update {maintable_name}
    set cdb_recency_pub = case when b.maxdate = a.consumer_db_orderdate_pub then 1 
							   else datediff(month,cast(a.consumer_db_orderdate_pub+'15' as timestamp),cast(b.maxdate+'15' as timestamp)) end
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on a.sourcelistid = b.sourcelistid
     --and (cdb_freq_orders_cat_hh >4 and cdb_freq_orders_cat_hh is not null) for auditing report filter
    and (a.consumer_db_orderdate_pub is not null and a.consumer_db_orderdate_pub not in ('',' ','000000'));
END;


/*
fields to be populated  for consumer_db_orderdate_np
cdb_recency_np
*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;
BEGIN;
create table prospector_tempcalculations_tobedropped 
   ( 
	sourcelistid int,    
	maxdate  varchar(6)
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (sourcelistid,maxdate)
select sourcelistid, max(consumer_db_orderdate_np) as maxdate
from {maintable_name}
where consumer_db_list_category in ('3','5')
group by sourcelistid
order by sourcelistid;
END;

/*
delete the blanks(empty records)
*/
BEGIN;
delete from prospector_tempcalculations_tobedropped where maxdate ='';
END;

/*
update tblmain
*/

BEGIN;
update {maintable_name}
    set cdb_recency_np = case when b.maxdate = a.consumer_db_orderdate_np then 1 
							   else datediff(month,cast(a.consumer_db_orderdate_np+'15' as timestamp),cast(b.maxdate+'15' as timestamp)) end
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on a.sourcelistid = b.sourcelistid
     --and (cdb_freq_orders_cat_hh >4 and cdb_freq_orders_cat_hh is not null) for auditing report filter
    and (a.consumer_db_orderdate_np is not null and a.consumer_db_orderdate_np not in ('',' ','000000'));
END;