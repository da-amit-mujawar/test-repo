--note: when len(company_id) or len(individual_id) = 12, then numeric
-- 20220914 CB: export only records that match cons univ and are active
-- 20220921 CB: export test files for matches/non-matches, actives/in-actives plus hh status field on all exports
-- 20221010 CB: revise to only send ACTIVES and NO hhs flag until test files approved.

--report count table
drop table if exists apogee_export_count;
create table apogee_export_count
    (tablename varchar(150), record_count bigint);


--create distinct household file from consumer universe
begin;
  drop table if exists ctas_cons_unv_compid_tobedropped;
    create table ctas_cons_unv_compid_tobedropped
    as
      (select distinct company_id,
             min(individual_id) as individual_id,
             max(nvl (lems,' ')) as lems,
             min(nvl (household_status_code_2010,' ')) as hh_status_code
      from {maintable_name}
      where household_status_code_2010 = 'F' -- 20221010 CB
      group by company_id);
end;


-- get distinct individuals from consumer universe
begin;
  drop table if exists ctas_unique_individ;
    create table ctas_unique_individ
    as
      (select distinct individual_id,
             max(company_id) as company_id,
             max(nvl (lems,' ')) as lems,
             min(nvl (household_status_code_2010,' ')) as hh_status_code
      from {maintable_name}
      where household_status_code_2010 = 'F' -- 20221010 CB
      group by individual_id);
end;


begin;
  -- get apg matches
  drop table if exists ctas_np_hh_match;
    create table ctas_np_hh_match
    as
      (select distinct apg.company_id as apg_hh_id,
             'Y' as donormatch
      from exclude_sum_ltd_np apg
        inner join ctas_unique_individ univ on apg.company_id = univ.company_id);
  -- get mgen matches
  drop table if exists ctas_mgen_hh_match;
    create table ctas_mgen_hh_match
    as
      (select distinct mgen.company_id as mgen_hh_id,
             'Y' as mgenmatch
      from exclude_nonprofit_child2_74 mgen
        inner join ctas_unique_individ univ on mgen.company_id = univ.company_id);
end;

  --create distinct individual file from consumer universe
begin;
  drop table if exists ctas_cons_unv_individ_tobedropped;
    create table ctas_cons_unv_individ_tobedropped
    as
      (select distinct individual_id,
             max(company_id) as company_id,
             nvl(lems,' ') as lems,
             nvl(hh_status_code,' ') as hh_status_code,
             nvl(donormatch,'N') as donormatch,
             nvl(mgenmatch,'N') as mgenmatch
      from ctas_unique_individ univ
        left outer join ctas_np_hh_match apg on univ.company_id = apg.apg_hh_id
        left outer join ctas_mgen_hh_match mgen on univ.company_id = mgen.mgen_hh_id
      group by individual_id,
               company_id,
               lems,
               hh_status_code,
               donormatch,
               mgenmatch);
end;


drop table if exists ctas_unique_individ;
drop table if exists ctas_np_hh_match;
drop table if exists ctas_mgen_hh_match;
