from datetime import datetime
from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *
import pandas as pd
from pandas.io import sql as psql
from sqlalchemy import create_engine
import logging
import sys

def update_build_info(databaseid, keyname, keyvalue, **kwargs):
    build_info = Variable.get('var-db-' + str(databaseid), deserialize_json=True, default_var=None)
    build_info[keyname] = keyvalue
    Variable.set(f'var-db-{str(databaseid)}', json.dumps(build_info, indent=4))
    logging.info(build_info)


def prospector_load_table(buildid, databaseid, maintablename, **kwargs):
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)
    redshift_hook = get_redshift_hook()

    # The list of fields are stored in a vaiable
    field_list = Variable.get('var-db-1094-fieldlist')
    createtable_sql = f"""DROP TABLE IF EXISTS {maintablename}; Create table {maintablename} ({field_list})"""
    logging.info(createtable_sql)
    redshift_hook.run(createtable_sql)

    # create a table to track loading errors and data issues
    createtable_sql = f"""DROP TABLE IF EXISTS prospector_load_error_log; 
                    Create table prospector_load_error_log 
                    (buildid int, 
                    listid int,
                    listname varchar (500),
                    sourcelistid int, 
                    sourcelistname varchar(500),
                    sourcedatabaseid int, 
                    sourcedatabasename varchar(500),
                    sourcetablename varchar(50),
                    errortype varchar(100),
                    errormsg varchar(8000),
                    errordate datetime
                    )"""
    redshift_hook.run(createtable_sql)

    insert_sql_p1 = f"""Insert Into {maintablename} ("""
    select_field_list = ''
    for item in field_list.split(', ')[:-3]:
        select_field_list = select_field_list + item.split(' ')[0] + ', \n'

    insert_sql_p1 = insert_sql_p1 + select_field_list[:-3] + ', SourceListID,  ListID)' + '\n'
    #logging.info(insert_sql_p1)

    with closing(sql_conn.cursor()) as sql_cursor:
        # Get all lists, listname, sourcelistid, filter, database, dbid, databasename, buildid, maintable name
        list_sql = f"""
            Select Distinct CONVERT(integer, MasterLoLID) as Listid, MasterLol.cListname,  
                    CONVERT(integer, MasterLol.cCode) as SourceListid, isnull(MasterLol.cCustomText10, '') as ListFilter, 
                    D.databaseid,  D.cListname as SourcelistName, b.cDatabaseName, M.BuildID, 
                    T.cTableName
            from tblBuildLoL  lol
                    inner join tblMasterLoL  MasterLol On MasterLol.ID = lol.MasterLoLID  
                    inner join tblMasterLoL D on CONVERT(integer, MasterLol.cCode) = D.ID
                    inner join tblDatabase B on D.databaseId = B.id
                    inner join (SELECT MAX(ID) as BuildID, DatabaseID FROM tblBuild where iIsOnDisk =1 and iIsReadyToUse = 1 GROUP BY DatabaseID) M
                                on B.id = M.DatabaseID
                    inner join tblBuildTable T on T.BuildID = M.BuildID
            where MasterLol.cCode <>''
            and MasterLol.iIsActive =1
            and LK_Action in ('R','N')
            and MasterLol.DatabaseID ={databaseid} --1094
            and lol.buildid={buildid} --20530 in dev
            and T.cTableName   like 'tblMain%' --in ('tblMain_20339_202106', 'tblMain_20502_202101', 'tblMain_20552_202004')
        """

        # loop through each database to fetch records and insert into prospector tblmain.
        sql_cursor.execute(list_sql)
        row = sql_cursor.fetchone()
        while row:
            ilistid, listname, sourcelistid, listfilter, sdatabaseid, sourcelistname, databasename, sbuildid, tablename = row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]
            listname, sourcelistname, databasename = listname.replace("'", "''"), sourcelistname.replace("'", "''"), databasename.replace("'", "''")
            logging.info(row)
            if listfilter:
                listfilter = f' and ({listfilter})'

            insert_sql_p2 = f'{insert_sql_p1} select {select_field_list} ListID, {ilistid} from {tablename} where ListID={sourcelistid}{listfilter}'
            logging.info(insert_sql_p2)
            try:
                logging.info(f'Insert records into prospector tblmain-source database: {databasename}, {databaseid}')
                redshift_hook.run(insert_sql_p2)
            except:
                logging.info(f'********ERROR************: Inserting records into prospector tblmain-source database: {databasename},databaseid: {databaseid}')
                logging.info(sys. exc_info())

                type, val, tb = sys.exc_info()

                error_log_sql = f"""Insert into prospector_load_error_log
                                    select {buildid}, {ilistid}, '{listname}', {sourcelistid}, '{sourcelistname}', 
                                    {sdatabaseid}, '{databasename}', '{tablename}', 'load', '{val}', getdate()
                            """
                redshift_hook.run(error_log_sql)
                pass

            row = sql_cursor.fetchone()

    # if there are records with empty Consumer_DB_Category, log it in the error table
    cat_error_sql = error_log_sql = f"""Insert into prospector_load_error_log (buildid, listid, sourcelistid, errortype, errormsg, errordate)
                                    select {buildid}, listid, sourcelistid, 'empty_Category', 'Consumer_DB_List_Category', getdate()
                                    from {maintablename} where nvl(Consumer_DB_List_Category) ='' 
                                    group by listid, sourcelistid having count(listid)>0
                                """
    redshift_hook.run(cat_error_sql)

    current_date = datetime.now()
    df_report = redshift_hook.get_pandas_df('select * from prospector_load_error_log')
    if not df_report.empty:
        audit_report_name = f"/tmp/Prospector-Load-Error-for-BuildID-{buildid}-{current_date.strftime('%Y')}{current_date.strftime('%m')}{current_date.strftime('%d')}.xlsx"
        df_report.to_excel(audit_report_name, index=False)
        update_build_info(databaseid, 'load_error_report', audit_report_name, **kwargs)
        raise ValueError('Error found in during loading prospector main table. Check prospector_load_error_log in Redshift for more information')
    else:
        update_build_info(databaseid, 'load_error_report', '', **kwargs)


def prospector_load_error_report(buildid, databaseid, maintablename, **kwargs):
    current_date = datetime.now()
    redshift_hook = get_redshift_hook()

    df_report = redshift_hook.get_pandas_df('select * from prospector_load_error_log')

    audit_report_name = f"/tmp/Prospector-Load-Error-for-BuildID-{buildid}-{current_date.strftime('%Y')}{current_date.strftime('%m')}{current_date.strftime('%d')}.xlsx"
    df_report.to_excel(audit_report_name, index=False)

    build_info = Variable.get('var-db-' + str(databaseid), deserialize_json=True, default_var=None)
    build_info["load_error_report"] = audit_report_name
    Variable.set(f'var-db-{str(databaseid)}', json.dumps(build_info, indent=4))
    logging.info(build_info)
    return [audit_report_name]
