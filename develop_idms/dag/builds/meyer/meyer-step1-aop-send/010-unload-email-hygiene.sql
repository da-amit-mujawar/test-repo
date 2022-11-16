-- fix email before sending to ehygiene and aop
update {maintable_name}
   set cemail = replace(replace(cemail,'"',''),',','')
 where nvl(cemail,'') != '';

-- send email for ehygiene
unload ('select distinct(rtrim(ltrim(cemail))) as email from {maintable_name} where nvl(cemail, '''') != ''''')
to 's3://{s3-aopinput}/{emailoversight-prefix}_d045000_myr_{yyyy}{mm}{dd}_'
iam_role '{iam}'
allowoverwrite
parallel off
;

unload ('select count(distinct cemail) from  {maintable_name} where nvl(cemail, '''') != ''''')
to 's3://{s3-internal}{reportname}'
iam_role '{iam}'
kms_key_id '{kmskey}'
encrypted
csv delimiter as '|'
allowoverwrite
header
parallel off
;
