INSERT INTO core_bf.{counts_table}
  SELECT sub.ftype,sub.cdate,sub.recordCount
  FROM
  (
    select '{file_type}' as ftype, current_date as cdate, count(*) as recordCount from core_bf.{file_type}
  )sub

