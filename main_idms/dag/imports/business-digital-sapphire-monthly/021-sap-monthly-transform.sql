




insert into {table_job_stats}
select 'Sap Email segmented',count(*),getdate() from {table_sap_email_segmented};
