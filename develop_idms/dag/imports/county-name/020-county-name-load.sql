--load from tblstate
insert into {tablename1}
    (cstatecode,ccountycode,ccountyname)
select distinct
	trim(upper(cstatecode)),
	trim(upper(ccountycode)),
	trim(upper(ccounty)) as ccountyname
from {table_state};

