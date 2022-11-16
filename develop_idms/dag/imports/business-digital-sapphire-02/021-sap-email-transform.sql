
--drop matches to table_sap_email
delete from {table_email_appended}
where individualmc+email in(select individualmc+email from {table_sap_email});

insert into {table_sap_email_fnl}
select distinct individualmc,email,min(listid) as listid
from {table_sap_email}
where listid in (select listid from {table_sap_listid})
group by individualmc,email;

insert into {table_sap_email_fnl}
select distinct individualmc,email,min(listid) as listid
from {table_email_appended}
group by individualmc,email;

insert into {table_job_stats}
select 'Sap Email final',count(*),getdate() from {table_sap_email_fnl};


-- create email segmented file for further processing
insert into {table_sap_email_segmented}

select  distinct a.individualmc,a.email,a.listid,
        b.SP001,b.SP002,b.SP031,b.SP040,b.SP041,b.SP043,
        b.SP045,b.SP125,b.SP134,b.SP143,
        b.SP146,b.SP147,b.SP149,b.SP237,b.SP239,b.SP241,
        b.SP242,b.SP243,b.SP245,b.SP247,b.SP248,b.SP249,
        b.SP250,b.SP251,b.SP252,b.SP253,b.SP254,b.SP256,
        b.SP257,b.SP259,b.SP261,b.SP030,b.SP036,b.SP037,
        b.SP047,b.SP049,b.SP050,b.SP051,b.SP052,b.SP054,
        b.SP055,b.SP056,b.SP057,b.SP062,b.SP060,b.SP063,
        b.SP066,b.SP067,b.SP068,b.SP129,b.SP138,b.SP142,
        b.SP069,b.SP070,b.SP061,b.SP277,b.SP276,
ROW_NUMBER() OVER (PARTITION BY email ORDER BY email asc) AS row_no
from {table_sap_email_fnl} a
left outer join {table_sap_bluekai} b
on a.IndividualMC=B.IndividualMC
ORDER BY 2;
--157373604

insert into {table_job_stats}
select 'Sap Email segmented',count(*),getdate() from {table_sap_email_segmented};
