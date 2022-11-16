-- Make a backup of tblmain
drop table if exists {maintable_name}_orig;
select * into {maintable_name}_orig from {maintable_name};

-- fix empty non-numeric schoolids from main table
update {maintable_name} set myrschoolid = '000'
where listid = 18702
    and myrschoolid not between '000' and '999';
update {maintable_name} set myrschoolid = right(concat('000',myrschoolid),3) ;

-- school table - set default values
update meyer_school_new set myrschoolid = right(concat('000',myrschoolid),3) ;
update meyer_school_new set myr_school_salutation_type = 'FORMAL' where nvl(myr_school_salutation_type,'') = '';
update meyer_school_new set myr_school_priority = '999' where nvl(myr_school_priority,'') = '';

-- set priority based on schoolid
update {maintable_name}
   set myrschoolpriority = myr_school_priority
  from {maintable_name} m join meyer_school_new s on m.myrschoolid = s.myrschoolid
 where nvl(m.myrschoolid,'') <> '';

-- fix flongitude (negatives) in main table
update {maintable_name} set flongitude = replace(flongitude,'-','') where flongitude < 0;

-- CB: 2021.08.06: uppercase nyl_met_flag per Lisa K
update meyer_state_availability_new set nyl_met_flag = upper(trim(nyl_met_flag)) where nvl(nyl_met_flag,'') <> '';

-- CB: 2021.08.07: proper case name fields used in aggregations that weren't part of aop process...
update {maintable_name}
   set raw_field_maidenname = initcap(trim(raw_field_maidenname))
from {maintable_name}
where nvl(raw_field_maidenname,'')<>'';

update {maintable_name}
   set raw_field_spousemaidenname = initcap(trim(raw_field_spousemaidenname))
from {maintable_name}
where nvl(raw_field_spousemaidenname,'')<>'';

update {maintable_name}
   set raw_field_salutation = initcap(trim(raw_field_salutation))
from {maintable_name}
where nvl(raw_field_salutation,'')<>'';

update {maintable_name}
   set raw_field_joint_salutation = initcap(trim(raw_field_joint_salutation))
from {maintable_name}
where nvl(raw_field_joint_salutation,'')<>'';

-- new flags for revised fullnamecreated fields: alumni/spouse
-- alter table {maintable_name} add column da_usetitle char(1);
-- alter table {maintable_name} add column da_usegensfx char(1);
-- alter table {maintable_name} add column da_usesuffix char(1);
-- alter table {maintable_name} add column da_usemaiden char(1);
-- alter table {maintable_name} add column da_usetitle_spouse char(1);
-- alter table {maintable_name} add column da_usegensfx_spouse char(1);
-- alter table {maintable_name} add column da_usesuffix_spouse char(1);
-- alter table {maintable_name} add column da_usemaiden_spouse char(1);
-- alter table {maintable_name} add column da_create_spousefn char(1);



