create or replace function udf_getdecodesfromdictionaryarray_multi(
input_string varchar(max),
input_separator varchar(3),
dict_array varchar(max)
)
returns varchar(max)
stable
as $$

try:
  stresult =''
  if input_string is None  or len(dict_array) ==0 :
    return stresult


  import json
  input_list = input_string.split(input_separator)
  dict_values = json.loads(dict_array)

  result_list = []
  for i in range(len(input_list)):
    strtocheck = input_list[i]
    if strtocheck in dict_values.keys() and dict_values.get(strtocheck) not in result_list :
        result_list.append (dict_values.get(strtocheck) )

    result_list.sort()
    stresult= ''
    for j in range(len(result_list)):
        stresult = stresult + result_list[j] + ','

    stresult= stresult.strip().replace(' ','')
    if stresult <>'':
      stresult = ','+ stresult

except:
   stresult=''

result = stresult
return result

$$ language plpythonu;
