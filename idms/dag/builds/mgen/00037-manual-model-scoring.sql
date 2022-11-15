drop table if exists mgenscore_tblchild9_317 CASCADE;
drop table if exists mgenscore_tblchild6_317 CASCADE;
drop table if exists mgenscore_tblchild10_317 CASCADE;
drop table if exists mgenscore_tblchild11_317 CASCADE;
drop table if exists mgenscore_tblchild21_317 CASCADE;
drop table if exists mgenscore_tblchild20_317 CASCADE;
drop table if exists mgenscore_tblchild7_317 CASCADE;
drop table if exists mgenscore_tblchild8_317 CASCADE;
drop table if exists mgenscore_tblchild12_317 CASCADE;
drop table if exists mgenscore_tblchild22_317 CASCADE;
drop table if exists mgenscore_tblchild23_317 CASCADE;
drop table if exists mgenscore_tblchild24_317 CASCADE;
drop table if exists mgenscore_tblchild25_317 CASCADE;
drop table if exists mgenscore_tblchild5_317 CASCADE;
drop table if exists mgenscore_tblchild4_317 CASCADE;
drop table if exists mgenscore_tblchild27_317 CASCADE;
drop table if exists mgenscore_tblchild28_317 CASCADE;
drop table if exists mgenscore_tblchild26_317 CASCADE;
drop table if exists mgenscore_317_p1 CASCADE;
drop table if exists tmpdistinctcid_317 CASCADE;
drop table if exists mgenscore_317 CASCADE;
drop table if exists mgenscore_tblchild11_317 CASCADE;
drop table if exists mgenscore_tblchild5_317 CASCADE;
drop table if exists mgencombined_317 CASCADE;
drop table if exists mgencombined_sum1_317 CASCADE;
drop table if exists mgencombined_sum_317 CASCADE;


create table mgenscore_317_p1 (cid varchar(18));

insert into mgenscore_317_p1 (cid)
select distinct maintable.cid
from tblmain_{build_id}_{build} maintable
left join tblchild2_{build_id}_{build} child2 on maintable.cid = child2.cid
left join tblchild1_{build_id}_{build} child1 on maintable.cid = child1.cid
where (last_transaction_catalog_days between 15 and 180 and (fin_age = 0 or fin_age between 30 and 79) and (ppi = 0 or ppi between 5000 and 99000));


create table tmpdistinctcid_317 (cid varchar(18));

insert into tmpdistinctcid_317 (cid)
select distinct p1.cid from mgenscore_317_p1 p1;

create table mgenscore_317 (id int, cid varchar(18), score int, deciles varchar(3));

insert into mgenscore_317 (id, cid, score)
select distinct maintable.id, maintable.cid, 747353 +
	    case when (scf between '754' and '759') or (scf between '763' and '769') or (scf between '776' and '779')
	             or  (scf between '783' and '785') or (scf between '790' and '799') or (scf = '885') then 27231 else 0 end +
        case when buyer_behavior_cluster_2004 = 'i' then 40741 else 0 end +
        case when cat_trns_12m >= 30 then 36972 else 0 end +
        case when fin_age between 45 and 64 then 67290 else 0 end +
        case when fin_gen = 'f' then 53979 else 0 end +
        case when fin_married between '0' and '3' then 35742 else 0 end +
        case when homevalue between 'a' and 'c' then 46243 else 0 end +
        case when last_transaction_overall_days between 15 and 30 then 22197 else 0 end +
        case when lengthofresidence >= 26 then -54328 else 0 end +
        case when rtl_parts = 0 then 38096 else 0 end +
        case when rtl_parts >= 3 then -85726 else 0 end +
        case when st in ('al', 'ar', 'ga', 'la', 'ms', 'nc', 'tn') then 27231 else 0 end +
        case when tot_parts >= 40 then -54487 else 0 end +
        case when wealthfinder = 'q' then 46095 else 0 end +
        case when wealthfinder = 'r' then 57491 else 0 end +
        case when wealthfinder = 's' then 83174 else 0 end +
        case when wealthfinder = 't' then 87630 else 0 end +
        case when wealthfinder between 'a' and 'i' then -102707 else 0 end +
        case when web_parts = 0 then 23559 else 0 end
from tblmain_{build_id}_{build} maintable
    left join tblchild2_{build_id}_{build} child2 on maintable.cid = child2.cid
	left join tblchild1_{build_id}_{build} child1 on maintable.cid = child1.cid
	inner join tmpdistinctcid_317 samplecid on samplecid.cid = maintable.cid
    where (last_transaction_catalog_days between 15 and 180 and (fin_age = 0 or fin_age between 30 and 79) and (ppi = 0 or ppi between 5000 and 99000)
);

update mgenscore_317
set score = score +
case when cctype_12m_visa = '1' or cctype_12m_mc = '1' or cctype_12m_amex = '1' or cctype_12m_disc = '1' or cctype_12m_btq = '1' then -40767 else 0 end +
case when pay_12m_db = '1' or pay_13p_db = '1' then 79844 else 0 end
from tblchild1_{build_id}_{build} b
where mgenscore_317.cid = b.cid;

create table mgenscore_tblchild11_317 (cid varchar(18), score int);

insert into mgenscore_tblchild11_317 (cid, score)
select distinct sampledata.cid, case when dnr.donor_flags like '%,3,%' then -65437 else 0 end
		from tblmain_{build_id}_{build} dnr
		inner join tmpdistinctcid_317 sampledata
        on dnr.cid = sampledata.cid;

create table mgenscore_tblchild5_317 (cid varchar(18), score int);

insert into mgenscore_tblchild5_317 (cid, score)
select distinct sampledata.cid,
    case when trn.trn_category = 'c020' and trn.trn_totaltransaction12month >= 1 then 42532 else 0 end +
    case when trn.trn_category = 'c047' and trn.trn_totaltransaction12month >= 1 then 26393 else 0 end +
    case when trn.trn_category = 'c119' and trn.trn_totaltransaction12month >= 1 then 29856 else 0 end +
    case when trn.trn_category in ('c018', 'c025', 'c037', 'c086', 'c105', 'c111', 'c113', 'c136', 'c160', 'c164')
        and trn.trn_totaltransaction12month >= 1 then -90395 else 0 end
from tblchild5_{build_id}_{build} trn inner join tmpdistinctcid_317 sampledata on trn.cid = sampledata.cid;


create table mgenscore_tblchild4_317 (cid varchar(18), score int);

insert into mgenscore_tblchild4_317 (cid, score)
select distinct sampledata.cid,
		   case when trs.trs_supercategory = 'sc04' and trs.trs_totaltransaction12month >= 1 then 31113 else 0 end
from tblchild4_{build_id}_{build} trs
inner join tmpdistinctcid_317 sampledata
on trs.cid = sampledata.cid;

create table if not exists mgenscore_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild9_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild6_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild10_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild11_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild20_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild19_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild21_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild7_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild8_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild12_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild22_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild23_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild24_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild25_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild5_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild4_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild27_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild28_317 (cid varchar(18), score int);
create table if not exists mgenscore_tblchild26_317 (cid varchar(18), score int);




create table mgencombined_317
(id int, cid varchar(18), score1 int,score2 int,score3 int,score4 int,score5 int,score6 int,score7 int,score8 int,score9 int,score10 int,score11 int,score12 int,score13 int,score14 int,score15 int,score16 int,score17 int,score18 int,score19 int,score20 int,score21 int,score22 int,score23 int,score24 int,score25 int,score26 int,score27 int,score28 int,score29 int);

insert into mgencombined_317 (id, cid, score1) select distinct id,cid,score from mgenscore_317;
insert into mgencombined_317 (cid, score2) select distinct cid,score from mgenscore_tblchild9_317;
insert into mgencombined_317 (cid, score3) select distinct cid,score from mgenscore_tblchild6_317;
insert into mgencombined_317 (cid, score4) select distinct cid,score from mgenscore_tblchild10_317;
insert into mgencombined_317 (cid, score5) select distinct cid,score from mgenscore_tblchild11_317;
insert into mgencombined_317 (cid, score6) select distinct cid,score from mgenscore_tblchild20_317;
insert into mgencombined_317 (cid, score7) select distinct cid,score from mgenscore_tblchild19_317;
insert into mgencombined_317 (cid, score8) select distinct cid,score from mgenscore_tblchild21_317;
insert into mgencombined_317 (cid, score9) select distinct cid,score from mgenscore_tblchild7_317;
insert into mgencombined_317 (cid, score10) select distinct cid,score from mgenscore_tblchild8_317;
insert into mgencombined_317 (cid, score11) select distinct cid,score from mgenscore_tblchild12_317;
insert into mgencombined_317 (cid, score12) select distinct cid,score from mgenscore_tblchild22_317;
insert into mgencombined_317 (cid, score13) select distinct cid,score from mgenscore_tblchild23_317;
insert into mgencombined_317 (cid, score14) select distinct cid,score from mgenscore_tblchild24_317;
insert into mgencombined_317 (cid, score15) select distinct cid,score from mgenscore_tblchild25_317;
insert into mgencombined_317 (cid, score16) select distinct cid,score from mgenscore_tblchild5_317;
insert into mgencombined_317 (cid, score17) select distinct cid,score from mgenscore_tblchild4_317;
insert into mgencombined_317 (cid, score18) select distinct cid,score from mgenscore_tblchild27_317;
insert into mgencombined_317 (cid, score19) select distinct cid,score from mgenscore_tblchild28_317;
insert into mgencombined_317 (cid, score20) select distinct cid,score from mgenscore_tblchild26_317;


-- sum up scores from all child tables
select cid, sum(isnull(score2,0) + isnull(score3,0)+  isnull(score4,0)+   isnull(score5,0)+   isnull(score6,0)  + isnull(score7,0)  +
			isnull(score8,0) + isnull(score9,0) + isnull(score10,0) + isnull(score11,0) + isnull(score12,0) + isnull(score13,0) +
			isnull(score14,0) + isnull(score15,0) + isnull(score16,0) + isnull(score17,0) + isnull(score18,0) +
			isnull(score19,0) + isnull(score20,0) + isnull(score21,0) + isnull(score22,0) + isnull(score23,0) + isnull(score24,0) +
			isnull(score25,0) + isnull(score26,0) + isnull(score27,0) + isnull(score28,0) + isnull(score29,0)) nchildsum
into mgencombined_sum1_317
from mgencombined_317
group by cid;

create table mgencombined_sum_317 (id int, cid varchar(18), nfinalscore int, ndeciles numeric(3,1));

insert into mgencombined_sum_317 (id, cid, nfinalscore)
select a.id,a.cid,isnull(a.score1,0) + cchild.nchildsum nfinalscore
from mgencombined_317 a
inner join mgencombined_sum1_317 cchild
on a.cid = cchild.cid and a.score1 is not null;

update mgencombined_sum_317 set ndeciles = case when nfinalscore >= 1124698 then 1.1
when nfinalscore >= 1087547 then 1.2
when nfinalscore >= 1063155 then 1.3
when nfinalscore >= 1045517 then 1.4
when nfinalscore >= 1029681 then 1.5
when nfinalscore >= 1016758 then 2.1
when nfinalscore >= 1006405 then 2.2
when nfinalscore >= 996576 then 2.3
when nfinalscore >= 987564 then 2.4
when nfinalscore >= 979363 then 2.5
when nfinalscore >= 972026 then 3.1
when nfinalscore >= 965168 then 3.2
when nfinalscore >= 958835 then 3.3
when nfinalscore >= 952970 then 3.4
when nfinalscore >= 946871 then 3.5
when nfinalscore >= 942272 then 4.1
when nfinalscore >= 935622 then 4.2
when nfinalscore >= 931102 then 4.3
when nfinalscore >= 925995 then 4.4
when nfinalscore >= 921531 then 4.5
when nfinalscore >= 899958 then 5
when nfinalscore >= 881886 then 6
when nfinalscore >= 863390 then 7
when nfinalscore >= 846805 then 8
when nfinalscore >= 831188 then 9
when nfinalscore >= 814504 then 10
when nfinalscore >= 799761 then 11
when nfinalscore >= 783095 then 12
when nfinalscore >= 764680 then 13
when nfinalscore >= 745401 then 14
when nfinalscore >= 722585 then 15
when nfinalscore >= 697713 then 16
when nfinalscore >= 669147 then 17
when nfinalscore >= 633386 then 18
when nfinalscore >= 578860 then 19
else 20 end;

drop table if exists tblchild57_{build_id}_{build} CASCADE;

create table tblchild57_{build_id}_{build} (id int, ndeciles numeric(3,1)) ;

insert into tblchild57_{build_id}_{build} (id, ndeciles)
select id, ndeciles from mgencombined_sum_317;