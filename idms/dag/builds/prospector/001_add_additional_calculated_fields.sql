BEGIN;
alter table {maintable_name} alter column fullname ENCODE zstd ;
alter table {maintable_name} alter column title ENCODE zstd ;
alter table {maintable_name} alter column firstname ENCODE zstd ;
alter table {maintable_name} alter column middle_initial ENCODE zstd ;
alter table {maintable_name} alter column lastname ENCODE zstd ;
alter table {maintable_name} alter column suffix ENCODE zstd ;
alter table {maintable_name} alter column company ENCODE zstd ;
alter table {maintable_name} alter column addressline1 ENCODE zstd ;
alter table {maintable_name} alter column addressline2 ENCODE zstd ;
alter table {maintable_name} alter column city ENCODE zstd ;
alter table {maintable_name} alter column state ENCODE  bytedict  ;
alter table {maintable_name} alter column zip9 ENCODE zstd ;
alter table {maintable_name} alter column lems ENCODE zstd ;
alter table {maintable_name} alter column individual_mc ENCODE zstd ;
alter table {maintable_name} alter column gender ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_list_category ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_product_category ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_magazine_type ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_nonprofit_category ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_source ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_payment_type ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_orderdate_cat ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_orderdate_pub ENCODE zstd ;
alter table {maintable_name} alter column consumer_db_orderdate_np ENCODE zstd ;
alter table {maintable_name} alter column permissiontype ENCODE zstd ;
alter table {maintable_name} alter column sourcelistid ENCODE zstd ;
alter table {maintable_name} alter column listid ENCODE zstd ;
alter table {maintable_name} alter distkey id, alter sortkey (id);
END;

BEGIN;
update {maintable_name}
    set consumer_db_orderdate_np =''
    where consumer_db_orderdate_np ='0';
END;

BEGIN;
alter table {maintable_name} add column cdb_orderdate_cat_ind varchar(6) ENCODE zstd;
alter table {maintable_name} add column cdb_freq_orders_cat_ind bigint ENCODE az64;
alter table {maintable_name} add column cdb_total_amount_cat_ind bigint ENCODE az64;
alter table {maintable_name} add column cdb_avg_amount_cat_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_num_lists_cat_ind int ENCODE zstd;
alter table {maintable_name} add column cdb_orderdate_pub_ind varchar(6) ENCODE lzo;
alter table {maintable_name} add column cdb_freq_orders_pub_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_total_amount_pub_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_avg_amount_pub_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_num_lists_pub_ind int ENCODE lzo;
alter table {maintable_name} add column cdb_orderdate_np_ind varchar(6) ENCODE lzo;
alter table {maintable_name} add column cdb_freq_orders_np_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_total_amount_np_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_avg_amount_np_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_num_lists_np_ind int ENCODE lzo;
alter table {maintable_name} add column cdb_orderdate_total_ind varchar(6) ENCODE zstd;
alter table {maintable_name} add column cdb_freq_orders_total_ind bigint ENCODE az64;
alter table {maintable_name} add column cdb_total_amount_total_ind bigint ENCODE az64;
alter table {maintable_name} add column cdb_avg_amount_total_ind bigint ENCODE lzo;
alter table {maintable_name} add column cdb_num_lists_total_ind int ENCODE zstd;
alter table {maintable_name} add column cdb_orderdate_cat_hh varchar(6) ENCODE zstd;
alter table {maintable_name} add column cdb_freq_orders_cat_hh bigint ENCODE az64;
alter table {maintable_name} add column cdb_total_amount_cat_hh bigint ENCODE zstd;
alter table {maintable_name} add column cdb_avg_amount_cat_hh bigint ENCODE zstd;
alter table {maintable_name} add column cdb_num_lists_cat_hh int ENCODE zstd;
alter table {maintable_name} add column cdb_orderdate_pub_hh varchar(6) ENCODE zstd;
alter table {maintable_name} add column cdb_freq_orders_pub_hh bigint ENCODE az64;
alter table {maintable_name} add column cdb_total_amount_pub_hh bigint ENCODE az64;
alter table {maintable_name} add column cdb_avg_amount_pub_hh bigint ENCODE lzo;
alter table {maintable_name} add column cdb_num_lists_pub_hh int ENCODE zstd;
alter table {maintable_name} add column cdb_orderdate_np_hh varchar(6) ENCODE lzo;
alter table {maintable_name} add column cdb_freq_orders_np_hh bigint ENCODE az64;
alter table {maintable_name} add column cdb_total_amount_np_hh bigint ENCODE lzo;
alter table {maintable_name} add column cdb_avg_amount_np_hh bigint ENCODE az64;
alter table {maintable_name} add column cdb_num_lists_np_hh int ENCODE az64;
alter table {maintable_name} add column cdb_orderdate_total_hh varchar(6) ENCODE zstd;
alter table {maintable_name} add column cdb_freq_orders_total_hh bigint ENCODE az64;
alter table {maintable_name} add column cdb_total_amount_total_hh bigint ENCODE az64;
alter table {maintable_name} add column cdb_avg_amount_total_hh bigint ENCODE zstd;
alter table {maintable_name} add column cdb_num_lists_total_hh int ENCODE zstd;
alter table {maintable_name} add column scf varchar(3) ENCODE zstd;
alter table {maintable_name} add column zip varchar(5) ENCODE zstd;
alter table {maintable_name} add column zipradius char(1) ENCODE lzo;
alter table {maintable_name} add column dropflag char(1) ENCODE lzo;
alter table {maintable_name} add column standard_scf_omits char(1) ENCODE zstd;
alter table {maintable_name} add column omit_states_vi_as char(1) ENCODE zstd;
alter table {maintable_name} add column canadian_record_flag char(1) ENCODE zstd;
END;

BEGIN;
update {maintable_name} set scf = left(zip9,3);
END;

BEGIN;
update {maintable_name} set zip = left(zip9,5);
END;

BEGIN;
update {maintable_name} set standard_scf_omits = case when left(zip9,3) in ('000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '090', '091', '092', '093', '094', '095','096', '097', '098', '099','340', '962', '963', '964','965', '966', '969', '987') then 'Y' else 'N' end;
END;

BEGIN;
update {maintable_name} set canadian_record_flag = case when substring(zip9,1,1) between 'A' and 'Z' then 'Y' else 'N' end;
END;

BEGIN;
update {maintable_name} set omit_states_vi_as  = case when state in ('AS','VI') then 'Y' else 'N' end;
END;

BEGIN;
select omit_states_vi_as,count(*) from {maintable_name} group by omit_states_vi_as;
END;

/*
rollups based on lems
*/

/*
--fields to be populated  for cat_hh
cdb_orderdate_cat_hh
cdb_freq_orders_cat_hh
cdb_total_amount_cat_hh
cdb_avg_amount_cat_hh    -- calculated  from total amt /sum of freq
cdb_num_lists_cat_hh

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (lems varchar(18),  
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum  bigint,
    consumer_db_total_amount_sum   bigint,
    sourcelistid_count int
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems,consumer_db_orderdate_max,consumer_db_freq_orders_sum , consumer_db_total_amount_sum, sourcelistid_count)
select
    lems, 
    max(consumer_db_orderdate_cat) as consumer_db_orderdate_max,
    sum(consumer_db_freq_orders_cat) as consumer_db_freq_orders_sum,
    sum(consumer_db_total_amount_cat) as consumer_db_total_amount_sum,
    count(distinct sourcelistid) as sourcelistid_count  
from {maintable_name}
where consumer_db_list_category ='2'
group by lems;
END;

BEGIN;
update {maintable_name}
  set cdb_orderdate_cat_hh  = b.consumer_db_orderdate_max,
    cdb_freq_orders_cat_hh  = b.consumer_db_freq_orders_sum,
    cdb_total_amount_cat_hh = b.consumer_db_total_amount_sum,
    cdb_num_lists_cat_hh = b.sourcelistid_count
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.lems)) <> '';
END;

/*
get the avg  for cdb_avg_amount_cat_hh    -- calculated  from total amt /sum of freq

get the avg  for cdb_avg_amount_cat_hh    -- calculated  from total amt /sum of freq
*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (lems varchar(18),  
    iaverage  bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems,iaverage)
select
    lems,  
    round(avg(cast(cdb_total_amount_cat_hh as float)/cdb_freq_orders_cat_hh),0) as iaverage
from {maintable_name}
where consumer_db_list_category ='2'
and (cdb_freq_orders_cat_hh <>0 and cdb_freq_orders_cat_hh is not null)
group by lems;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_cat_hh  = b.iaverage 
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.iaverage is not null
and ltrim(rtrim(a.lems)) <> '';
END;

/*fields to be populated  for pub_hh

cdb_orderdate_pub_hh
cdb_freq_orders_pub_hh
cdb_total_amount_pub_hh
cdb_avg_amount_pub_hh    -- calculated  from total amt /sum of freq
cdb_num_lists_pub_hh

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
 (lems varchar(18),  
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum  bigint,
    consumer_db_total_amount_sum   bigint,
    sourcelistid_count int
 );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems,consumer_db_orderdate_max,consumer_db_freq_orders_sum , consumer_db_total_amount_sum, sourcelistid_count)
select
    lems, 
    max(consumer_db_orderdate_pub) as consumer_db_orderdate_max,
    sum(consumer_db_freq_orders_pub) as consumer_db_freq_orders_sum,
    sum(consumer_db_total_amount_pub) as consumer_db_total_amount_sum,
    count(distinct sourcelistid) as sourcelistid_count 
from {maintable_name}
where consumer_db_list_category in ('1','4','6')
group by lems;
END;

BEGIN;
update {maintable_name}
  set cdb_orderdate_pub_hh  = b.consumer_db_orderdate_max,
    cdb_freq_orders_pub_hh  = b.consumer_db_freq_orders_sum,
    cdb_total_amount_pub_hh = b.consumer_db_total_amount_sum,
    cdb_num_lists_pub_hh =  b.sourcelistid_count
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.lems)) <> '';
END;

/*
get the avg  for cdb_avg_amount_pub_hh    -- calculated  from total amt /sum of freq
*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (lems varchar(18),  
    iaverage  bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems,iaverage)
select
    lems,  
    round(avg(cast(cdb_total_amount_pub_hh as float)/cdb_freq_orders_pub_hh),0) as iaverage
from {maintable_name}
where consumer_db_list_category in ('1','4','6')
and (cdb_freq_orders_pub_hh <>0 and cdb_freq_orders_pub_hh is not null)
group by lems;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_pub_hh  = b.iaverage 
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.iaverage is not null
and ltrim(rtrim(a.lems)) <> '';
END;
/*
fields to be populated  for np_hh

cdb_orderdate_np_hh
cdb_freq_orders_np_hh
cdb_total_amount_np_hh
cdb_avg_amount_np_hh    -- calculated  from total amt /sum of freq
cdb_num_lists_np_hh

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/


BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (lems varchar(18),  
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum  bigint,
    consumer_db_total_amount_sum   bigint,
    sourcelistid_count int
    
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems,consumer_db_orderdate_max,consumer_db_freq_orders_sum , consumer_db_total_amount_sum, sourcelistid_count)
select
    lems, 
    max(consumer_db_orderdate_np) as consumer_db_orderdate_max,
    sum(consumer_db_freq_orders_np) as consumer_db_freq_orders_sum,
    sum(consumer_db_total_amount_np) as consumer_db_total_amount_sum,
    count(distinct sourcelistid) as sourcelistid_count
from {maintable_name}
where consumer_db_list_category in ('3','5')
group by lems;
END;

BEGIN;
update {maintable_name}
  set cdb_orderdate_np_hh  = b.consumer_db_orderdate_max,
    cdb_freq_orders_np_hh  = b.consumer_db_freq_orders_sum,
    cdb_avg_amount_np_hh = b.consumer_db_total_amount_sum,
    cdb_num_lists_np_hh = b.sourcelistid_count
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.lems)) <> '';
END;

/*
get the avg  for cdb_avg_amount_np_hh    -- calculated  from total amt /sum of freq
*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (lems varchar(18),  
    iaverage  bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems,iaverage)
select
    lems,  
    round(avg(cast(cdb_total_amount_np_hh as float)/cdb_freq_orders_np_hh),0) as iaverage
from {maintable_name}
where consumer_db_list_category in ('3','5')
and (cdb_freq_orders_np_hh <>0 and cdb_freq_orders_np_hh is not null)
group by lems;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_np_hh  = iaverage 
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.iaverage is not null
and ltrim(rtrim(a.lems)) <> '';
END;

/* fields to be populated  for "_total_hh" - max, sum, sum, count 

cdb_orderdate_total_hh
cdb_freq_orders_total_hh
cdb_total_amount_total_hh
cdb_num_lists_total_hh

select top 100 * from dba.prospector_tempcalculations_tobedropped;
*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (lems varchar(18),  
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum bigint,
    consumer_db_total_amount_sum bigint,
    sourcelistid_count int   
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems,consumer_db_orderdate_max, consumer_db_freq_orders_sum, consumer_db_total_amount_sum, sourcelistid_count)
    select lems,
max(
     case when consumer_db_orderdate_cat >= consumer_db_orderdate_pub then 
     case when (consumer_db_orderdate_cat >= consumer_db_orderdate_np) then consumer_db_orderdate_cat else consumer_db_orderdate_np end
     else case when (consumer_db_orderdate_pub >= consumer_db_orderdate_np)  then consumer_db_orderdate_pub  else consumer_db_orderdate_np end
     end ) as consumer_db_orderdate_max,

sum (consumer_db_freq_orders_cat + consumer_db_freq_orders_pub + consumer_db_freq_orders_np)  as  consumer_db_freq_orders_sum,

sum (consumer_db_total_amount_cat + consumer_db_total_amount_pub + consumer_db_total_amount_np) as  consumer_db_total_amount_sum,

count(distinct sourcelistid) as cdb_num_lists_total_hh

from {maintable_name}
group by lems
;
END;


BEGIN;
update {maintable_name}
  set cdb_orderdate_total_hh = b.consumer_db_orderdate_max,
      cdb_freq_orders_total_hh = b.consumer_db_freq_orders_sum,
      cdb_total_amount_total_hh = b.consumer_db_total_amount_sum,
      cdb_num_lists_total_hh = b.sourcelistid_count
      from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.lems)) <> '';
END;



/* fields to be populated  for "_total_hh" - avg (calculated  from total amt /sum of freq)

 cdb_avg_amount_total_hh

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;

create table prospector_tempcalculations_tobedropped 
   (lems varchar(18),  
    iaverage bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (lems, iaverage)
    select lems, round(avg(cast(cdb_total_amount_total_hh as float)/cdb_freq_orders_total_hh),0) as iaverage
 from {maintable_name}
where cdb_freq_orders_total_hh <>0 and cdb_freq_orders_total_hh is not null
group by lems;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_total_hh  = iaverage
      from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.lems)) = ltrim(rtrim(b.lems))
and  b.iaverage is not null
and ltrim(rtrim(a.lems)) <> '';
END;


/*
rollups based on indivdual_mc
*/


/*
--fields to be populated  for cat_ind
cdb_orderdate_cat_ind
cdb_freq_orders_cat_ind
cdb_total_amount_cat_ind
cdb_avg_amount_cat_ind    -- calculated  from total amt /sum of freq
cdb_num_lists_cat_ind

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum  bigint,
    consumer_db_total_amount_sum   bigint,
    sourcelistid_count int
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc,consumer_db_orderdate_max,consumer_db_freq_orders_sum , consumer_db_total_amount_sum, sourcelistid_count)
select
    individual_mc, 
    max(consumer_db_orderdate_cat) as consumer_db_orderdate_max,
    sum(consumer_db_freq_orders_cat) as consumer_db_freq_orders_sum,
    sum(consumer_db_total_amount_cat) as consumer_db_total_amount_sum,
    count(distinct sourcelistid) as sourcelistid_count  
from {maintable_name}
where consumer_db_list_category ='2'
group by individual_mc;
END;

BEGIN;
update {maintable_name}
  set cdb_orderdate_cat_ind  = b.consumer_db_orderdate_max,
    cdb_freq_orders_cat_ind  = b.consumer_db_freq_orders_sum,
    cdb_total_amount_cat_ind = b.consumer_db_total_amount_sum,
    cdb_num_lists_cat_ind = b.sourcelistid_count
    
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;

/*
get the avg  for cdb_avg_amount_cat_ind    -- calculated  from total amt /sum of freq
*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    iaverage  bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc,iaverage)
select
    individual_mc,  
    round(avg(cast(cdb_total_amount_cat_ind as float) /cdb_freq_orders_cat_ind),0) as iaverage
from {maintable_name}
where consumer_db_list_category ='2'
and (cdb_freq_orders_cat_ind <>0 and cdb_freq_orders_cat_ind is not null)
group by individual_mc;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_cat_ind  = iaverage 
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.iaverage is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;

/*
--fields to be populated  for pub_ind

cdb_orderdate_pub_ind
cdb_freq_orders_pub_ind
cdb_total_amount_pub_ind
cdb_avg_amount_pub_ind    -- calculated  from total amt /sum of freq
cdb_num_lists_pub_ind

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/


BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum  bigint,
    consumer_db_total_amount_sum   bigint,
    sourcelistid_count int
     
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc,consumer_db_orderdate_max,consumer_db_freq_orders_sum , consumer_db_total_amount_sum, sourcelistid_count)
select
    individual_mc, 
    max(consumer_db_orderdate_pub) as consumer_db_orderdate_max,
    sum(consumer_db_freq_orders_pub) as consumer_db_freq_orders_sum,
    sum(consumer_db_total_amount_pub) as consumer_db_total_amount_sum,
    count(distinct sourcelistid) as sourcelistid_count 
from {maintable_name}
where consumer_db_list_category in ('1','4','6')
group by individual_mc;
END;

BEGIN;
update {maintable_name}
  set cdb_orderdate_pub_ind  = b.consumer_db_orderdate_max,
    cdb_freq_orders_pub_ind  = b.consumer_db_freq_orders_sum,
    cdb_total_amount_pub_ind = b.consumer_db_total_amount_sum,
    cdb_num_lists_pub_ind =  b.sourcelistid_count
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;

/*
get the avg  for cdb_avg_amount_pub_ind    -- calculated  from total amt /sum of freq
*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    iaverage  bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc,iaverage)
select
    individual_mc,  
    round(avg(cast(cdb_total_amount_pub_ind as float) /cdb_freq_orders_pub_ind),0) as iaverage
from {maintable_name}
where consumer_db_list_category in ('1','4','6')
and (cdb_freq_orders_pub_ind <>0 and cdb_freq_orders_pub_ind is not null)
group by individual_mc;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_pub_ind  = iaverage 
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.iaverage is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;

/*
--fields to be populated  for np_ind

cdb_orderdate_np_ind
cdb_freq_orders_np_ind
cdb_total_amount_np_ind
cdb_avg_amount_np_ind    -- calculated  from total amt /sum of freq
cdb_num_lists_np_ind

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/


BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;

create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum  bigint,
    consumer_db_total_amount_sum   bigint,
    sourcelistid_count int
    
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc,consumer_db_orderdate_max,consumer_db_freq_orders_sum , consumer_db_total_amount_sum, sourcelistid_count)
select
    individual_mc, 
    max(consumer_db_orderdate_np) as consumer_db_orderdate_max,
    sum(consumer_db_freq_orders_np) as consumer_db_freq_orders_sum,
    sum(consumer_db_total_amount_np) as consumer_db_total_amount_sum,
    count(distinct sourcelistid) as sourcelistid_count
from {maintable_name}
where consumer_db_list_category in ('3','5')
group by individual_mc;
END;

BEGIN;
update {maintable_name}
  set cdb_orderdate_np_ind  = b.consumer_db_orderdate_max,
    cdb_freq_orders_np_ind  = b.consumer_db_freq_orders_sum,
    cdb_avg_amount_np_ind = b.consumer_db_total_amount_sum,
    cdb_num_lists_np_ind = b.sourcelistid_count
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;

/*
get the avg  for cdb_avg_amount_np_ind    -- calculated  from total amt /sum of freq
*/
BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    iaverage  bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc,iaverage)
select
    individual_mc,  
    round(avg(cast(cdb_total_amount_np_ind as float) /cdb_freq_orders_np_ind),0) as iaverage
from {maintable_name}
where consumer_db_list_category in ('3','5')
and (cdb_freq_orders_np_ind <>0 and cdb_freq_orders_np_ind is not null)
group by individual_mc;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_np_ind  = iaverage 
from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.iaverage is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;



/* fields to be populated  for "_total_ind" - max, sum, sum, count 

cdb_orderdate_total_ind
cdb_freq_orders_total_ind
cdb_total_amount_total_ind
cdb_num_lists_total_ind

select top 100 * from dba.prospector_tempcalculations_tobedropped;
*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    consumer_db_orderdate_max  varchar(6),
    consumer_db_freq_orders_sum bigint,
    consumer_db_total_amount_sum bigint,
    sourcelistid_count int   
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc,consumer_db_orderdate_max, consumer_db_freq_orders_sum, consumer_db_total_amount_sum, sourcelistid_count)
    select individual_mc,  
max(
     case when consumer_db_orderdate_cat >= consumer_db_orderdate_pub then 
     case when (consumer_db_orderdate_cat >= consumer_db_orderdate_np) then consumer_db_orderdate_cat else consumer_db_orderdate_np end
     else case when (consumer_db_orderdate_pub >= consumer_db_orderdate_np)  then consumer_db_orderdate_pub  else consumer_db_orderdate_np end
end ) as consumer_db_orderdate_max,

sum (consumer_db_freq_orders_cat + consumer_db_freq_orders_pub + consumer_db_freq_orders_np)  as  consumer_db_freq_orders_sum,

sum (consumer_db_total_amount_cat + consumer_db_total_amount_pub + consumer_db_total_amount_np) as  consumer_db_total_amount_sum,

count(distinct sourcelistid) as cdb_num_lists_total_ind

from {maintable_name}
group by individual_mc
;
END;

BEGIN;
update {maintable_name}
  set cdb_orderdate_total_ind = b.consumer_db_orderdate_max,
      cdb_freq_orders_total_ind = b.consumer_db_freq_orders_sum,
      cdb_total_amount_total_ind = b.consumer_db_total_amount_sum,
      cdb_num_lists_total_ind = b.sourcelistid_count
      from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.consumer_db_orderdate_max is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;

/* fields to be populated  for "_total_ind" - avg (calculated  from total amt /sum of freq)

 cdb_avg_amount_total_ind

select top 100 * from dba.prospector_tempcalculations_tobedropped;

*/

BEGIN;
drop table if exists prospector_tempcalculations_tobedropped;
END;

BEGIN;
create table prospector_tempcalculations_tobedropped 
   (individual_mc varchar(27),
    iaverage bigint
   );
END;

BEGIN;
insert into prospector_tempcalculations_tobedropped (individual_mc, iaverage)
    select individual_mc, round(avg(cast(cdb_total_amount_total_ind as float) /cdb_freq_orders_total_ind),0) as iaverage
 from {maintable_name}
where cdb_freq_orders_total_ind <>0 and cdb_freq_orders_total_ind is not null
group by individual_mc;
END;

BEGIN;
update {maintable_name}
  set cdb_avg_amount_total_ind  = iaverage
      from {maintable_name} a
inner join prospector_tempcalculations_tobedropped b on  ltrim(rtrim(a.individual_mc)) = ltrim(rtrim(b.individual_mc))
and  b.iaverage is not null
and ltrim(rtrim(a.individual_mc)) <> '';
END;

/*
reju m 2017.06.09.  pipe in individual_mc cause problems while running orders.  ticket# 624201 
*/
BEGIN;
delete from {maintable_name}  where individual_mc like '%|%'  ;
END;
