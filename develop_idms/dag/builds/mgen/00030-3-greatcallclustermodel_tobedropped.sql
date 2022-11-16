drop table if exists greatcallclustermodel_tobedropped CASCADE;

create table greatcallclustermodel_tobedropped 
(
   id int, 
   fin_age int,
   internetusage varchar(2),
   earlynetx double precision,
   custagex  double precision,
    gc_distance1  double precision,
    gc_distance2 double precision,
    gc_distance3 double precision,
    gc_distance4 double precision,
   ndeciles numeric(1)
);

insert into greatcallclustermodel_tobedropped 
(
    id,
    fin_age,internetusage
)
select 
  id,
  case when fin_age_old <> '' then cast(fin_age_old as int) else 0 end,
  internetusage 
from mgen_new_load tblmain
left join tblchild2_{build_id}_{build} tblchild2  on tblmain.cid = tblchild2.cid
where fin_age >= 50;

update greatcallclustermodel_tobedropped
 set earlynetx = case 
  when internetusage = '01' then -2.5277096650
  when internetusage = '02' then -2.1083459120
  when internetusage = '03' then -1.6889821590
  when internetusage = '04' then -1.2696184070
  when internetusage = '05' then -0.8502546540
  when internetusage = '06' then -0.4308909010
  when internetusage = '07' then -0.0115271490
  when internetusage = '08' then  0.4078366041
  when internetusage = '09' then  0.8272003567
  when internetusage = '10' then  1.2465641094
 else  0.4078366041 end,
 
 custagex = case
  when 50 <= fin_age and fin_age <= 53 then -1.2782886250
  when 54 <= fin_age and fin_age <= 57 then -0.9058788530
  when 58 <= fin_age and fin_age <= 61 then -0.5334690800
  when 62 <= fin_age and fin_age <= 65 then -0.1610593080
  when 66 <= fin_age and fin_age <= 69 then  0.2113504646
  when 70 <= fin_age and fin_age <= 73 then  0.5837602370
  when 74 <= fin_age and fin_age <= 77 then  0.9561700094
  when 78 <= fin_age and fin_age <= 81 then  1.3285797818
  when 82 <= fin_age and fin_age <= 85 then  1.7009895543
  when 86 <= fin_age and fin_age <= 89 then  2.0733993267
  when 90 <= fin_age and fin_age <= 99 then  2.4458090991
else  0 end;

update greatcallclustermodel_tobedropped
 set 
gc_distance1 = 
        case when earlynetx <> 0 then power(earlynetx -  0.611241979,2) else 0 end + 
        case when custagex <> 0 then power(custagex -  0.698543180,2) else 0 end 
,
gc_distance2 = 
        case when earlynetx <> 0 then power(earlynetx - -1.618551967,2) else 0 end + 
        case when custagex <> 0 then power(custagex - -1.406612167,2) else 0 end 
,
gc_distance3 = 
        case when earlynetx <> 0 then power(earlynetx -  0.339467954,2) else 0 end + 
        case when custagex <> 0 then power(custagex - -0.888799632,2) else 0 end 
,
gc_distance4 = 
        case when earlynetx <> 0 then power(earlynetx - -1.160112294,2) else 0 end + 
        case when custagex <> 0 then power(custagex -  0.603051943,2) else 0 end ;

update greatcallclustermodel_tobedropped 
  set ndeciles = case 
       when gc_distance1 <= gc_distance2 and gc_distance1 <= gc_distance3 and gc_distance1 <= gc_distance4  then 1
       when gc_distance2 <= gc_distance1 and gc_distance2 <= gc_distance3 and gc_distance2 <= gc_distance4  then 2
       when gc_distance3 <= gc_distance1 and gc_distance3 <= gc_distance2 and gc_distance3 <= gc_distance4  then 3
       when gc_distance4 <= gc_distance1 and gc_distance4 <= gc_distance2 and gc_distance4 <= gc_distance3  then 4
 else 0 end;