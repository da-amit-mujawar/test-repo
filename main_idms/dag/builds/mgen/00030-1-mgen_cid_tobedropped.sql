drop table if exists mgen_cid_tobedropped CASCADE;
create table mgen_cid_tobedropped 
(
    cid varchar(18) DISTKEY SORTKEY
);

insert into mgen_cid_tobedropped
select cid from mgen_new_load group by cid;