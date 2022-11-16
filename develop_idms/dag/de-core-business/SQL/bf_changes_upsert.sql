-- Delete the existing data based on id from the main table
DELETE FROM core_bf.{table}
WHERE {primary_key_var} IN (SELECT {primary_key_var} FROM core_bf.{table}_changes);

-- Insert New IDs
INSERT INTO core_bf.{table} ( {column_list} )
  SELECT {column_list}
  FROM core_bf.{table}_changes
  WHERE {primary_key_var} NOT IN (SELECT {primary_key_var} FROM core_bf.{table});