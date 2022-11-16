-- remove empty codes and descriptions
delete FROM {tablename1}
 where trim(titleid) = ''
    or trim(titledescription) = '';

-- format description as upper
update {tablename1}
   set titledescription = upper(trim(titledescription));

-- remove preceding zeros from titleid for search
alter table {tablename1} add column titleid_var varchar(8) default '';

update {tablename1}
   set titleid_var = cast(cast(titleid as integer) as varchar(8));





