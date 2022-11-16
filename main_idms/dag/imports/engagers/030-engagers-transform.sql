-- remove empty values in join columns
delete from {tablename1}
 where EmailAddress in ('','email');

-- add new field for Last Click/Open Date
alter table {tablename1} add column openclickdate VARCHAR(20) not null default '';

--defaults to open date which is all populated and usually LESS than clickdate... is this correct?
update {tablename1}
set openclickdate = left(opendate,6)
from {tablename1}
where opendate <> '';
--55,712,218

--~11 million LESS than clickdate... is this correct?
update {tablename1}
set openclickdate = left(clickdate,6)
from {tablename1}
where opendate = '' and openclickdate = '' and clickdate <> ''  --0
--where clickdate <> '' and clickdate > openclickdate;  --11,270,399
;

--populate distinct table
insert into {tabledistinct1} (EmailAddress, OpenClickDate)
    select upper(left(ltrim(rtrim(EmailAddress)),65)) as EmailAddress,
           max(openclickdate) as OpenClickDate
    from {tablename1}
    group by EmailAddress
    order by EmailAddress;


