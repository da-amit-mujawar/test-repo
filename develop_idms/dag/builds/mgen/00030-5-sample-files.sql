drop table if exists #modelsampleidsprovided1percent CASCADE;
create table #modelsampleidsprovided1percent 
(
    lrfsid int 
);


--id file provided by user.  change the file name
COPY #modelsampleidsprovided1percent
from  's3://{s3-internal}/neptune/mGen/Mgen_IDsForModelSampleExport1Percent.txt'
iam_role '{iam}'
fixedwidth
'lrfsid:20';


drop table if exists onepercent_modelsample_{build_id} CASCADE;
select (a.id) id, a.cid
into onepercent_modelsample_{build_id}
from tblmain_{build_id}_{build} a
inner join #modelsampleidsprovided1percent b   on a.lrfsid = b.lrfsid
;

---50k model export load

drop table if exists #modelsampleidsprovided CASCADE;
create table #modelsampleidsprovided50k (lrfsid int );

--id file provided by user.  change the file name
COPY #modelsampleidsprovided50k
from  's3://{s3-internal}/neptune/mGen/Mgen_IDsForModelSampleExport50K.txt'
iam_role '{iam}'
fixedwidth
'lrfsid:20';

drop table if exists modelsample_{build_id} CASCADE;
select (a.id) id, a.cid
into modelsample_{build_id}
from tblmain_{build_id}_{build} a
inner join #modelsampleidsprovided50k  b on a.lrfsid = b.lrfsid
;


drop table #modelsampleidsprovided50k CASCADE;
drop table #modelsampleidsprovided1percent CASCADE;
