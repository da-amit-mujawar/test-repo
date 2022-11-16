
-- drop current tables
drop table if exists tblchild1_{build_id}_{build} ;
drop table if exists tblchild2_{build_id}_{build} ;
drop table if exists tblchild3_{build_id}_{build} ;
drop table if exists tblchild4_{build_id}_{build} ;
drop table if exists tblchild5_{build_id}_{build} ;
drop table if exists tblchild6_{build_id}_{build} ;


-- activate new tables
alter table meyer_school_new rename to tblchild1_{build_id}_{build};
alter table meyer_rate_chart_new rename to tblchild2_{build_id}_{build};
alter table meyer_first_degree_new rename to tblchild3_{build_id}_{build};
alter table meyer_generic_salutation_new rename to tblchild4_{build_id}_{build};
alter table meyer_state_availability_new rename to tblchild5_{build_id}_{build};
alter table meyer_title_standardization_new rename to tblchild6_{build_id}_{build};



