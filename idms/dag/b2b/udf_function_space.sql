create or replace function SPACE(ilen int)
returns varchar(max)
stable
as $$
str = ""
try:
  if ilen > 0:
    str = str.ljust(ilen)
except:
  str =""
return str
$$ language plpythonu;
