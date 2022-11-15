-- Step 5: Export MCD File as per MCD Input Layout
-- (s3://idms-7933-aop-input/IDMSAOPPI1/a05_meyer_mcd_input.txt)

unload ('select * from meyer_mcd_input;')
to 's3://idms-7933-aop-input/IDMSAOPPI1/a05_meyer_mcd_input.txt'
iam_role '{iam}'
csv delimiter as ';'
encrypted
parallel off
allowoverwrite
;

