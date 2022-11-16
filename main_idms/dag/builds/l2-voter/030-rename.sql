-- backup and rename
alter table {tablename2}
    rename to {tablename2}_tobedropped;
alter table {tablename1}
    rename to {tablename2};

-- create view for tblmain
drop view if exists public.{maintable_name};
create view public.{maintable_name} as
select l2_state as state, *
from public.{tablename2} with no schema binding;

