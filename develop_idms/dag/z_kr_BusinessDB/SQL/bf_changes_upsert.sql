-- Update Existing IDs
UPDATE core_bf.{table}
SET {set_columns}
FROM core_bf.{table}_changes c
WHERE core_bf.{table}.infogroup_id = c.infogroup_id;

-- Insert New IDs
INSERT INTO core_bf.{table}
  SELECT {column_list}
  FROM core_bf.{table}_changes
  WHERE infogroup_id NOT IN (SELECT infogroup_id FROM core_bf.{table});