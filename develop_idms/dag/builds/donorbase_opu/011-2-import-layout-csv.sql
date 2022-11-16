--stored procedure
CREATE OR REPLACE PROCEDURE sp_fetch_donorbase_opu_orderid()
language plpgsql AS
$$
DECLARE
    layout_rec RECORD;
    load_query VARCHAR(1000);
    load_query1 VARCHAR(1000);
    load_query2 VARCHAR(1000);
    load_query3 VARCHAR(1000);
    load_query4 VARCHAR(1000);
    load_query5 VARCHAR(1000);
BEGIN
    FOR layout_rec IN SELECT col001 FROM templayout where substring(col001,10,9) = 'Full Name'
    LOOP
        load_query :=  'drop table if exists donorb_{order_id}_order';
        load_query1 := '{crte_stmt1}';
        load_query2 := '{copy_command1}';
        load_query3 := 'drop table if exists donorb_{order_id}_order_exp';
        load_query4 := 'CREATE TABLE DonorB_{order_id}_order_exp AS
                        select
                        cast(''{order_id}'' as varchar(20)) order_id,
                        ltrim(rtrim(full_name)) fullname,
                        cast('''' as varchar(20)) first_name,
                        cast('''' as varchar(30)) last_name,
                        ltrim(rtrim(address_line_1)) address1,
                        ltrim(rtrim(address_line_2)) address2,
                        ltrim(rtrim(city)) city,
                        ltrim(rtrim(state)) state,
                        ltrim(rtrim(zip_code)) zip
                        from DonorB_{order_id}_order';
        load_query5 := 'unload (''select * from DonorB_{order_id}_order_exp'')
                        to ''s3://{s3-aopinput}/A08.{order_id}''
                        iam_role ''{iam}''
                        csv delimiter as ''|''
                        ALLOWOVERWRITE
                        header
                        parallel off';
               EXECUTE load_query;
               EXECUTE load_query1;
               EXECUTE load_query2;
               EXECUTE load_query3;
               EXECUTE load_query4;
               EXECUTE load_query5;
    END LOOP;

    FOR layout_rec IN SELECT col001 FROM templayout where substring(col001,10,10) = 'First Name'
    LOOP
        load_query :=  'drop table if exists donorb_{order_id}_order';
        load_query1 := '{crte_stmt1}';
        load_query2 := '{copy_command1}';
        load_query3 := 'drop table if exists donorb_{order_id}_order_exp';
        load_query4 := 'CREATE TABLE DonorB_{order_id}_order_exp AS
                        select
                        cast(''{order_id}'' as varchar(20)) order_id,
                        cast('''' as varchar(50)) fullname,
                        ltrim(rtrim(first_name)) first_name,
                        ltrim(rtrim(last_name)) last_name,
                        ltrim(rtrim(address_line_1)) address1,
                        ltrim(rtrim(address_line_2)) address2,
                        ltrim(rtrim(city)) city,
                        ltrim(rtrim(state)) state,
                        ltrim(rtrim(zip_code)) zip
                        from DonorB_{order_id}_order';
        load_query5 := 'unload (''select * from DonorB_{order_id}_order_exp'')
                        to ''s3://{s3-aopinput}/A08.{order_id}''
                        iam_role ''{iam}''
                        csv delimiter as ''|''
                        ALLOWOVERWRITE
                        header
                        parallel off';
               EXECUTE load_query;
               EXECUTE load_query1;
               EXECUTE load_query2;
               EXECUTE load_query3;
               EXECUTE load_query4;
               EXECUTE load_query5;
    END LOOP;
END
$$
;

CALL sp_fetch_donorbase_opu_orderid();
DROP PROCEDURE sp_fetch_donorbase_opu_orderid();

drop table if exists donorb_{order_id}_order;
drop table if exists DonorB_{order_id}_order_exp;
drop table if exists templayout;