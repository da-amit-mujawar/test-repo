-- remove empty values in join columns
delete from {tablename1}
 where cstatecode = '' or ccountycode = '' or czip = '';


