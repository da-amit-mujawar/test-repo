COPY {table_name}
{without_system_column}
FROM '{manifest_file}'
MANIFEST
IAM_ROLE '{iam}'
DELIMITER '|'
GZIP
TRUNCATECOLUMNS
ACCEPTINVCHARS
TRIMBLANKS
MAXERROR 1000;


