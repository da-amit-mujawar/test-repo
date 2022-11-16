drop table if exists meyer_model_score;

create table meyer_model_score (id int, ce_individual_id varchar(12), ce_household_id varchar(12), nscore numeric(11,9), ndeciles int);

insert into meyer_model_score (id, ce_individual_id, ce_household_id, nscore)
select   a.id, a.ce_individual_id, a.ce_household_id,
  CAST(1/(1+1/EXP(-6.3287 +
    CASE WHEN (LENGTH_OF_RESIDENCE >= '00' AND LENGTH_OF_RESIDENCE <= '03') THEN 1
         WHEN (LENGTH_OF_RESIDENCE >= '04' AND LENGTH_OF_RESIDENCE <= '09') THEN 2 ELSE 3 END  * -0.1767 +
    CASE WHEN DISCRETIONARY_INCOME_SCORE <> '' THEN CASE WHEN CAST(DISCRETIONARY_INCOME_SCORE AS INT) >= 95 THEN 1 ELSE 0 END ELSE 0 END  * -0.4285 +
    CASE WHEN TRIM(HOME_SALE_DATE) <> '' THEN CASE WHEN (DATE_PART_YEAR(CURRENT_DATE) - CAST(LEFT(HOME_SALE_DATE,4) AS INT)) <= 4 THEN 1 ELSE 0 END ELSE 0 END * 0.3297 +
    CASE WHEN TRIM(myrschoolpriority) <> '' THEN CASE WHEN CAST(TRIM(myrschoolpriority) AS INT) >= 40 THEN 1 ELSE 0 END ELSE 0 END * -0.3906 +
    CASE WHEN TRIM(myrfinalalumnigender) IN ('M','Male','m') THEN 1 ELSE 0 END * 0.3031 +
    CASE WHEN pdegreedoctorate <> '' THEN CASE WHEN CAST(pdegreedoctorate AS INT) / 100 >= 3.17 THEN 1 ELSE 0 END ELSE 0 END * 0.3337 +
    CASE WHEN MARKETTARGETAGE18_44FEMALE <> '' THEN CASE WHEN CAST(MARKETTARGETAGE18_44FEMALE AS INT) >= 7 THEN 1 ELSE 0 END ELSE 0 END * 0.2437 +
    CASE WHEN pinformation <> '' THEN CAST(pinformation AS INT) / 100 ELSE 0 END * -0.0331 +
    CASE WHEN CAST(TW_HEAVYCOUPONUSER AS INT) >= 4 THEN 1 ELSE 0 END  * -0.2008 +
    CASE WHEN HOME_AGE <> '' THEN CASE WHEN CAST(HOME_AGE AS INT) >= 50 THEN 1 ELSE 0 END ELSE 0 END  * -0.1945))
  AS Numeric(11,9))
 from {maintable_name} a
inner join {latest_maintablename} b
   on a.ce_household_id = b.company_id;

update meyer_model_score
   set ndeciles =
         case when nscore >= 0.001926703 then 1
              when nscore >= 0.001558589 then 2
              when nscore >= 0.001344975 then 3
              when nscore >= 0.001172403 then 4
              when nscore >= 0.001046332 then 5
              when nscore >= 0.000924419 then 6
              when nscore >= 0.000817042 then 7
              when nscore >= 0.000704022 then 8
              when nscore >= 0.000568302 then 9
              when nscore <  0.000568302 then 10
              else 99 end;


update {maintable_name}
   set myr_model_score = nvl(b.nscore, 0), myr_model_deciles = nvl(b.ndeciles, 99)
  from meyer_model_score b
 where {maintable_name}.id = b.id;

update {maintable_name}
   set myr_model_score = 0, myr_model_deciles = 99
 where myr_model_score is null;
