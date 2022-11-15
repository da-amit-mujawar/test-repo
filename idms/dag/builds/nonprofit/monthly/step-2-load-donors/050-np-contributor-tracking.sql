/* IDMS 2172 NonProfit Contributor Tracking
    - Summarizes Contributions by Build, List and Time-period buckets
    - Source: NonProfit Transaction tables
    - Run as part of monthly build
    - Inserts list summaries into exclude_np_contributor_tracking table in redshift
    - SOURCE FOR IDMS CAMPAIGN UI DASHBOARD REPORT
 */
delete from exclude_np_contributor_tracking where buildid = {build_id};

insert into exclude_np_contributor_tracking
    (databaseid, buildid, dcreateddate, listid, clientname, top20flag, qtrly_or_better, activeflag,
    m03, m06, m12, Upd_Q2, Upd_Q3, Upd_Q1, Upd_Q4, inactive_last_12mos,
    listaction, firsttransdate, lasttransdate, fullfilesize,
    m01_m03, m04_m06, m07_m09, m10_m12, m13_m24, m25_m48, m49_plus)

    with #listconvaudit as
    (
     select lca.databaseid
        , lca.buildid
        , cast(lca.dcreateddate as date) as dcreateddate
        , lca.listid
        , trim(upper(replace(replace(mlol.cListName,',',' '),'|',' '))) as clientname
        , CASE blol.LK_Action WHEN 'A' THEN 'Add' WHEN 'D' THEN 'Delete' WHEN 'O' THEN 'ReUse' WHEN 'R' THEN 'Replace' WHEN 'N' THEN 'New' ELSE '' END listaction
        , lca.dmaxtransactiondate
        , case when datediff(month, lca.dmaxtransactiondate, lca.dcreateddate) <= 12 then 1 else 0 end as activeflag
        , case when datediff(month, lca.dmaxtransactiondate, lca.dcreateddate) >  12 then 1 else 0 end as inactive_last_12mos
     from exclude_listconversion_audit lca
        inner join sql_tblmasterlol mlol on lca.listid = mlol.id
        inner join sql_tblBuildLoL BLoL on lca.buildid = blol.buildid and lca.listid = blol.masterlolid
     where lca.databaseid = {dbid}
        and lca.buildid = {build_id}
    ),
    #transactions as
    (
     select  sourcelistid as listid
              , min(detail_donationdate) firsttransdate
              , max(detail_donationdate) lasttransdate
              , cast(count(*) as bigint) fullfilesize
              , sum(case when datediff(month, cast(detail_donationdate as date), l.dcreateddate) between  0 and  3 then 1 else 0 end) as m01_m03
              , sum(case when datediff(month, cast(detail_donationdate as date), l.dcreateddate) between  4 and  6 then 1 else 0 end) as m04_m06
              , sum(case when datediff(month, cast(detail_donationdate as date), l.dcreateddate) between  7 and  9 then 1 else 0 end) as m07_m09
              , sum(case when datediff(month, cast(detail_donationdate as date), l.dcreateddate) between 10 and 12 then 1 else 0 end) as m10_m12
              , sum(case when datediff(month, cast(detail_donationdate as date), l.dcreateddate) between 13 and 24 then 1 else 0 end) as m13_m24
              , sum(case when datediff(month, cast(detail_donationdate as date), l.dcreateddate) between 25 and 48 then 1 else 0 end) as m25_m48
              , sum(case when datediff(month, cast(detail_donationdate as date), l.dcreateddate) > 48 then 1 else 0 end) as m49_plus
       from  exclude_nonprofit_transactions_{dbid} t
             inner join #listconvaudit l on t.sourcelistid = l.listid
      group by sourcelistid
      order by 1
    ),
    #ranktrans as
    (
     select  listid
              , fullfilesize
              , row_number() over (order by fullfilesize desc) as ranknbr
          from  #transactions
    ),
    #findtop as
    (
     select  listid
              , case when ranknbr <=20 then 1 else 0 end as top20flag
          from  #ranktrans
    ),
    #quarterly as
    (
     select  listid
             , case when (m01_m03 > 0 and m04_m06 > 0 and m07_m09 > 0 and m10_m12 > 0) then 1 else 0 end as qtrly_or_better
          from  #transactions
       group by listid, m01_m03, m04_m06, m07_m09, m10_m12
    ),
    #donations as
    (
     select  listid
            , case when  m01_m03 > 0           then 1 else 0 end as M03
            , case when (m01_m03+m04_m06) > 0  then 1 else 0 end as M06
            , case when (m01_m03+m04_m06+m07_m09+m10_m12) > 0  then 1 else 0 end as M12
            , case when (m01_m03+m04_m06+m07_m09+m10_m12) = 0 and (m13_m24+m25_m48+m49_plus) > 0 then 1 else 0 end as M13_plus_only
          from  #transactions
       group by listid, m01_m03, m04_m06, m07_m09, m10_m12, m13_m24, m25_m48, m49_plus
    ),
    #updates as
    (
     select  listid
            , case when m01_m03 > 0 then 1 else 0 end as Upd_Q1
            , case when (m01_m03 = 0 and m04_m06 > 0) then 1 else 0 end as Upd_Q2
            , case when (m01_m03 = 0 and m04_m06 = 0 and m07_m09 > 0) then 1 else 0 end as Upd_Q3
            , case when (m01_m03 = 0 and m04_m06 = 0 and m07_m09 = 0 and m10_m12 > 0) then 1 else 0 end as Upd_Q4
          from  #transactions
       group by listid, m01_m03, m04_m06, m07_m09, m10_m12
    )
     select lca.databaseid
            , lca.buildid
            , lca.dcreateddate
            , lca.listid
            , lca.clientname
            , ftop.top20flag
            , qtr.qtrly_or_better
            , lca.activeflag
            , don.M03
            , don.M06
            , don.M12
            , upd.Upd_Q1
            , upd.Upd_Q2
            , upd.Upd_Q3
            , upd.Upd_Q4
            , lca.inactive_last_12mos
            , lca.listaction
            , trans.firsttransdate
            , trans.lasttransdate
            , trans.fullfilesize
            , trans.m01_m03
            , trans.m04_m06
            , trans.m07_m09
            , trans.m10_m12
            , trans.m13_m24
            , trans.m25_m48
            , trans.m49_plus
      from  #listconvaudit lca
            inner join #transactions trans on lca.listid = trans.listid
            inner join #findtop ftop on lca.listid = ftop.listid
            inner join #quarterly qtr on lca.listid = qtr.listid
            inner join #donations don on lca.listid = don.listid
            inner join #updates upd   on lca.listid = upd.listid
;

unload ('select * from exclude_np_contributor_tracking;')
    to 's3://idms-7933-temp/exclude_np_contributor_tracking.csv'
    iam_role '{iam}'
    encrypted
    parallel off
    allowoverwrite
    csv
    delimiter as '|'
;

