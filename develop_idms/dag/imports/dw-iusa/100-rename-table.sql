 DROP TABLE IF EXISTS {maintable_name} CASCADE;
ALTER TABLE {iusa-stage-table} RENAME TO {maintable_name};