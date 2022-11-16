--load from tblstate
insert into {tablename1}
    (ccode,cdescription)
select distinct
	trim(upper(cstatecode))+trim(upper(ccountycode)) as ccode,
	trim(upper(ccounty)) as cdescription
from {table_state}
where not ccountycode in ('00','');

