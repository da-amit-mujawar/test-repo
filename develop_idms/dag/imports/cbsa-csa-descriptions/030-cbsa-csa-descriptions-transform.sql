-- remove empty values in join columns
delete from {table_cbsa_1} where cbsacode = '' or cbtitle = '';
delete from {table_csa_1} where csacode = '' or csa_name = '';

