/*
Political donor file (NP)
Data Source: np_pol_trans, consuniv ON individual_id
Condition: pol cat populated, donor OR mGen match
Order By: individual_id
Other: Pass 0 for nulls, space for char1,

 Update Schedule: Monthly

-- 20220921 CB: export test files for matches/non-matches, actives/in-actives plus hh status field on all exports

*/


drop table if exists ctas_tbldonorflag_tobedropped;
   select c.individual_id,
          c.listcategory03
     into ctas_tbldonorflag_tobedropped
     from tblChild3_{build_id}_{build} c
          inner join ctas_cons_unv_individ_tobedropped m on c.individual_id = m.individual_id
    where nvl(trim(listcategory03)) != ''
      and nvl(trim(c.individual_id,'')) != ''
group by c.individual_id, listcategory03
   having count(*) >0
 order by c.individual_id;

drop table if exists apogeepoliticalpivot_tobedropped;
create table apogeepoliticalpivot_tobedropped
(
	individual_id varchar(25),
	EC varchar(1),
	ED varchar(1),
	ER varchar(1),
	EV varchar(1),
	FC varchar(1),
	GC varchar(1),
	LG varchar(1),
	PC varchar(1),
	PL varchar(1),
	SA varchar(1),
	SE varchar(1),
	SC varchar(1),
	TP varchar(1),
	WA varchar(1)
);

insert into apogeepoliticalpivot_tobedropped
(
	individual_id,
	EC,
	ED,
	ER,
	EV,
	FC,
	GC,
	LG,
	PC,
	PL,
	SA,
	SE,
	SC,
	TP,
	WA
)
select individual_id ,
    max (case when listcategory03 = 'EC' then 'Y' else 'N' end) EC,
    max (case when listcategory03 = 'ED' then 'Y' else 'N' end) ED,
    max (case when listcategory03 = 'ER' then 'Y' else 'N' end) ER,
    max (case when listcategory03 = 'EV' then 'Y' else 'N' end) EV,
    max (case when listcategory03 = 'FC' then 'Y' else 'N' end) FC,
    max (case when listcategory03 = 'GC' then 'Y' else 'N' end) GC,
    max (case when listcategory03 = 'LG' then 'Y' else 'N' end) LG,
    max (case when listcategory03 = 'PC' then 'Y' else 'N' end) PC,
    max (case when listcategory03 = 'PL' then 'Y' else 'N' end) PL,
    max (case when listcategory03 = 'SA' then 'Y' else 'N' end) SA,
    max (case when listcategory03 = 'SE' then 'Y' else 'N' end) SE,
    max (case when listcategory03 = 'SC' then 'Y' else 'N' end) SC,
    max (case when listcategory03 = 'TP' then 'Y' else 'N' end) TP,
    max (case when listcategory03 = 'WA' then 'Y' else 'N' end) WA
from ctas_tbldonorflag_tobedropped
group by individual_id
order by individual_id;

drop table if exists ctas_political_donor_tobedropped;
create table ctas_political_donor_tobedropped
as
select
    a.individual_id,
    company_id,
    lems,
    'Y' as individualdonormatch,
    case when EC is null then 'N' else EC end as EC,
    case when ED is null then 'N' else ED end as ED,
    case when ER is null then 'N' else ER end as ER,
    case when EV is null then 'N' else EV end as EV,
    case when FC is null then 'N' else FC end as FC,
    case when GC is null then 'N' else GC end as GC,
    case when LG is null then 'N' else LG end as LG,
    case when PC is null then 'N' else PC end as PC,
    case when PL is null then 'N' else PL end as PL,
    case when SA is null then 'N' else SA end as SA,
    case when SE is null then 'N' else SE end as SE,
    case when SC is null then 'N' else SC end as SC,
    case when TP is null then 'N' else TP end as TP,
    case when WA is null then 'N' else WA end as WA
--    ,hh_status_code  -- 20221010 CB
from ctas_cons_unv_individ_tobedropped a
left join apogeepoliticalpivot_tobedropped b
on a.individual_id = b.individual_id
where (donormatch = 'Y' or mgenmatch = 'Y')
and nvl(a.individual_id,'') != ''--not on maintable
;

unload ('select * from ctas_political_donor_tobedropped order by individual_id')
    to 's3://{s3-apogee}{export_mmdb_1}'
    iam_role '{iam}'
    encrypted
    cleanpath
    --parallel off -- for order by clause
    gzip
    delimiter as '|'
    maxfilesize 6 gb;


insert into apogee_export_count
    (tablename, record_count)
select 'nonprofit_political', count(*)
from ctas_political_donor_tobedropped;

