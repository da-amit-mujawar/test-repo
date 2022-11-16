drop table if exists mgen_arrays CASCADE;

create table mgen_arrays (
cid varchar(18),
children_age_presence_flag varchar(30),
interest_flag varchar(150),
lifestyle_hobby_interest_flags varchar(270),
ailments_flags varchar(50),
credit_card_flag varchar(50),
donor_flags varchar(12),
market_target_age_flags varchar(80),
children_month_of_birth varchar(36),
children_age_by_gender varchar(150),
experian_donor_array varchar(120),
experian_child_array varchar(90),
experian_lifestyle_array varchar(210),
arr_twm varchar(500),
dlx_segments varchar(500),
arr_pay varchar(42),
arr_cc varchar(42)
)
DISTSTYLE KEY
DISTKEY(cid)
SORTKEY(cid);

insert into mgen_arrays
select a.cid,
       b.children_age_presence_flag,
       c.interest_flag,
       d.lifestyle_hobby_interest_flags,
       e.ailments_flags,
       f.credit_card_flag,
       g.donor_flags,
       h.market_target_age_flags,
       i.children_month_of_birth,
       j.children_age_by_gender,
       k.experian_donor_array,
       l.experian_child_array,
       m.experian_lifestyle_array,
       n.arr_twm,
       o.dlx_segments,
       p.arr_pay,
       r.arr_cc
from mgen_cid_tobedropped a
    left outer join mgen_children_age_presence_flag b on a.cid=b.cid
    left outer join mgen_interest_flag c on a.cid=c.cid
    left outer join mgen_lifestyle_hobby_interest_flags d on a.cid=d.cid
    left outer join mgen_ailments_flags e on a.cid=e.cid
    left outer join mgen_credit_card_flag f on a.cid=f.cid
    left outer join mgen_donor_flags g on a.cid=g.cid
    left outer join mgen_market_target_age_flags h on a.cid=h.cid
    left outer join mgen_children_month_of_birth i on a.cid=i.cid
    left outer join mgen_children_age_by_gender j on a.cid=j.cid
    left outer join mgen_experian_donor_array k on a.cid=k.cid
    left outer join mgen_experian_child_array l on a.cid=l.cid
    left outer join mgen_experian_lifestyle_array m on a.cid=m.cid
    left outer join mgen_arr_twm n on a.cid=n.cid
    left outer join mgen_dlx_segments o on a.cid=o.cid
    left outer join mgen_arr_pay p on a.cid=p.cid
    left outer join mgen_arr_cc r on a.cid=r.cid;
