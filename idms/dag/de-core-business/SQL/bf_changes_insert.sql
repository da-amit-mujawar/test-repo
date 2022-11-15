-- Insert New IDs
INSERT INTO core_bf.{table} ( {column_list} )
  SELECT {column_list}
  FROM core_bf.{table}_changes;
