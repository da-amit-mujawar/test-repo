--IDMS-2466 202210 Add new calculation for pin prefix
-- create reference table
drop table if exists meyer_pin_tobedropped;
create table meyer_pin_tobedropped(
    id varchar (20),
    myrschoolid varchar (3),
    prefix_pin char (2),    --202210
    new_pin char (6),
    create_date timestamp not null default getdate()
);

-- create pins on reference table
-- insert into meyer_pin_tobedropped (id, myrschoolid, new_pin)
insert into meyer_pin_tobedropped (id, myrschoolid, prefix_pin, new_pin)
    select id, myrschoolid ,
           concat(case to_char(getdate(),'mm')
                    when '01' then 'A' when '02' then 'B' when '03' then 'C' when '04' then 'D'
                    when '05' then 'E' when '06' then 'F' when '07' then 'G' when '08' then 'H'
                    when '09' then 'I' when '10' then 'J' when '11' then 'K' when '12' then 'L' else '' end,
                    case to_char(getdate(),'yy')
                    when '22' then 'A' when '23' then 'B' when '24' then 'C' when '25' then 'D'
                    when '26' then 'E' when '27' then 'F' when '28' then 'G' when '29' then 'H'
                    when '30' then 'I' when '31' then 'J' when '32' then 'K' when '33' then 'L'
                    when '34' then 'M' when '35' then 'N' when '36' then 'O' when '37' then 'P'
                    when '38' then 'Q' when '39' then 'R' when '40' then 'S' when '41' then 'T'
                    when '42' then 'U' when '43' then 'V' when '44' then 'W' when '45' then 'X'
                    when '46' then 'Y' when '47' then 'Z' else '' end) as prefix_pin,
           right(cast(row_number() over (
               partition by myrschoolid  order by myrschoolid, id) + 1000000 as char(7)),6) new_pin
      from {maintable_name}
    order by myrschoolid, id;

-- update tblmain with existing pins
-- IDMS-1579 CB 2021.10.28
update {maintable_name}
    --set dapin = new_pin
    --set dapin = concat(concat(to_char(getdate(),'mm'), chr(45)),left(new_pin,6))
  set dapin = concat(concat(prefix_pin,chr(45)),left(new_pin,6))
from {maintable_name} a
    inner join meyer_pin_tobedropped b on a.id = b.id;
