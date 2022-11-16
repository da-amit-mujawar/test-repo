-- Create matching ID in return table, it was modified in aop-step2
alter table meyer_mcd_return add column dwid bigint;

update meyer_mcd_return
    set dwid = convert(bigint,right(sequence,9))
from meyer_mcd_return;

-- Apply MCDs to current table (new build)
update {maintable_name}
   set
    mcdindividualid = nvl(b.enterpriseid,''),
    mcdhouseholdid = nvl(b.collection,'')
from {maintable_name} a inner join
    meyer_mcd_return b on a.id = b.dwid
where b.file_no = '200';

