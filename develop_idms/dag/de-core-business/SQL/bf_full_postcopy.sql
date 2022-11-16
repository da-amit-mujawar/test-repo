-- Rename existing table as Backup
DROP TABLE IF EXISTS core_bf.{table}_tobedropped CASCADE;
ALTER TABLE core_bf.{table} RENAME TO {table}_tobedropped;

-- Rename new table to prod table if everything goes well
ALTER TABLE core_bf.{table}_new RENAME TO {table};