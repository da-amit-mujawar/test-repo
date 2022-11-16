--Email suppress
UPDATE {maintable_name}
   SET cInclude = 'G'
  FROM {maintable_name} a INNER JOIN
    exclude_gst_suppressemails  b  ON a.EmailAddress = b.EmailAddress
 WHERE a.cInclude in ('Y','M')
   AND b.suppression_type <> 'R';
