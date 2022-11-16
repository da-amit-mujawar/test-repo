--Rename the stage table
DROP TABLE IF EXISTS {igus-raw-table};
DROP TABLE IF EXISTS {maintable_name} CASCADE;
ALTER TABLE {igus-stage-table} RENAME TO {maintable_name};