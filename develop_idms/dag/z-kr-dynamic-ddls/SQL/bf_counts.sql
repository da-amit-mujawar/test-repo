INSERT INTO core_bf.{counts_table}(filetype, today) VALUES({filetype}, current_date);

-- Update Existing IDs
UPDATE core_bf.{counts_table}
SET noOfRecords = subq.recordcount
FROM (
  select count(*) as recordcount from core_bf.{file_type}
)subq
WHERE core_bf.{table}.filetype = "{file_type}";

