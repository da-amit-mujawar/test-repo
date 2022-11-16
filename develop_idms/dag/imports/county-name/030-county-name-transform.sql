-- remove empty values in join columns
delete from {tablename1}
 where trim(cstatecode) = ''
    or trim(ccountycode) = ''
    or trim(ccountyname) = '';



