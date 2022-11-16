-- drop current table
drop table if exists {table_division_2};
alter table {table_division_1} rename to {table_division_2};

drop table if exists {table_database_2};
alter table {table_database_1} rename to {table_database_2};

drop table if exists {table_owner_2};
alter table {table_owner_1} rename to {table_owner_2};

drop table if exists {table_masterlol_2};
alter table {table_masterlol_1} rename to {table_masterlol_2};

drop table if exists {table_build_2};
alter table {table_build_1} rename to {table_build_2};

drop table if exists {table_buildtable_2};
alter table {table_buildtable_1} rename to {table_buildtable_2};

drop table if exists {table_buildtablelayout_2};
alter table {table_buildtablelayout_1} rename to {table_buildtablelayout_2};

drop table if exists {table_buildlol_2};
alter table {table_buildlol_1} rename to {table_buildlol_2};

drop table if exists {table_builddd_2};
alter table {table_builddd_1} rename to {table_builddd_2};

drop table if exists {table_extbuildtbldb_2};
alter table {table_extbuildtbldb_1} rename to {table_extbuildtbldb_2};

drop table if exists {table_dddesc_2};
alter table {table_dddesc_1} rename to {table_dddesc_2};

drop table if exists {table_dddesc_iq_2};
alter table {table_dddesc_iq_1} rename to {table_dddesc_iq_2};