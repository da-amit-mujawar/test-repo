--unload files for sql, pipe delimited

unload ('select * from {table_sap_bluekai}')
to 's3://digital-7933-business/output/{table_sap_bluekai}'
iam_role '{iam}'
allowoverwrite
csv delimiter as '|'
parallel off
;

unload ('select * from {table_sap_email_segmented}')
to 's3://digital-7933-business/output/{table_sap_email_segmented}'
iam_role '{iam}'
allowoverwrite
csv delimiter as '|'
parallel off
;