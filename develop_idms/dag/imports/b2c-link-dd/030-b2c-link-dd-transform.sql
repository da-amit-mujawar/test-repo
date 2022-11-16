-- remove empty values in join columns
delete from {tablename1}
 where trim(cfieldname) = ''
    or trim(cvalue) = ''
    or trim(cdescription) = '';

-- format columns as required
update {tablename1}
   set cfieldname = upper(trim(cfieldname)),
	   cvalue = upper(trim(cvalue)),
	   cdescription = initcap(trim(cdescription));
