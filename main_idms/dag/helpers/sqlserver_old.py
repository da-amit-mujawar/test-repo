from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
import logging
import json
from datetime import datetime

class myDict(dict):
    def __init__(self):
        self = dict()
    def add(self, key, value):
        self[str(key)] = str(value)



def get_build_info(databaseid, active_flag=0, **kwargs):
    """
    :param databaseid:
    :param active_flag: 0 for inactive build and 1 for active build
    :param kwargs:
    :return:
    """

    build_info_dic = Variable.get('var-db-' + str(databaseid), deserialize_json=True, default_var=None)

    # remove old build info - leave database id and name, clear all other fields
    for key in build_info_dic:
        if key not in ('dbid', 'database_name'):
            if key in ('build_id', 'pre_buildid'):
                build_info_dic[key] = 0
            else:
                build_info_dic[key] = ''

    if active_flag==1: 
        active_where = f"AND a.ID IN (SELECT MAX(ID) FROM tblBuild WHERE iIsOnDisk = 1 AND DatabaseID = {databaseid})"
    else: 
        active_where = f"AND a.ID IN (SELECT MAX(ID) FROM tblBuild WHERE iIsOnDisk = 0 AND DatabaseID = {databaseid})"

    sp_sql = f"""SELECT TOP 1  b.cTableName MainTableName, 
                    a.DatabaseID DBID,
                    a.ID BuildID,
                    d.cDatabaseName DatabaseName,
                    a.cDescription BuildDescription,
                    a.cBuild Build,
                    'UPDATE_' + CAST(a.DatabaseID as varchar) + '_' + CAST(a.ID as varchar) + '.zip' InputFileName,
                    @@SERVERNAME SQLServerName,
                    a.iPreviousBuildID PreviousBuildID,
                    ISNULL((SELECT TOP 1 cBuild FROM tblBuild WHERE ID = a.iPreviousBuildID), '') PreviousCBuild
                    FROM tblBuild a
                    INNER JOIN tblBuildTable b
                    ON a.ID = b.BuildID
                    INNER JOIN tblBuildTableLayout c
                    ON b.ID = c.BuildTableID
                    INNER JOIN tblDatabase d
                    ON d.ID = a.DatabaseID
                    WHERE a.DatabaseID = {databaseid}
                    AND a.iIsOnDisk = {active_flag}
                    AND a.iIsReadyToUse = 1
                    AND b.cTableName LIKE 'tblMain%'
                    {active_where}
                    ORDER BY a.ID DESC	"""

    logging.info('SqlServerOperator is checking credentials')
    sqlhook= get_sqlserver_hook()
    logging.info(f'Executing sql server query: {sp_sql}')
    build_info = sqlhook.get_first(sp_sql)
    logging.info(build_info)
    logging.info(build_info[0])
    if build_info:
        build_info_dic["maintable_name"] = build_info[0]
        build_info_dic["dbid"] = build_info[1]
        build_info_dic["build_id"] = build_info[2]
        build_info_dic["database_name"] = build_info[3]
        build_info_dic["build_desc"] = build_info[4]
        build_info_dic["build"] = build_info[5]
        build_info_dic["inputfile_name"] = build_info[6]
        build_info_dic["sqlserver_name"] = build_info[7]
        build_info_dic["pre_buildid"] = build_info[8]
        build_info_dic["pre_build"] = build_info[9]

    Variable.set(f'var-db-{str(databaseid)}', json.dumps(build_info_dic, indent=4))
    return build_info


def activate_build(database_id, build_status, min_expected_count, count_task_id, sql_conn_id, **kwargs):
    sqlhook= get_sqlserver_hook()
    table_record_count =kwargs['ti'].xcom_pull(task_ids=count_task_id)
    logging.info(f'Main table record count: {str(table_record_count)}')

    if table_record_count < min_expected_count:
        raise ValueError('Main table record count is too low')

    else:
        logging.info('Updating tblbuild ...')
        activate_sql = f"""
            UPDATE TOP(1) tblBuild SET LK_BuildStatus ={build_status}, iRecordCount ={table_record_count},
            dScheduledDateTime ='{str(datetime.now())[:22]}', dModifiedDate ='{str(datetime.now())[:22]}', cModifiedBy = 'Airflow'
            WHERE ID = {Variable.get('var-db-' + str(database_id),deserialize_json=True)['build_id']}
            """
        try:
            logging.info(f'Update query: {activate_sql}')
            sqlhook.run(activate_sql)
        except:
           logging.info('Error updating tblebuild')
           raise ValueError('Error activating the build')

def get_sqlserver_hook(**kwargs):
    sqlhook = MsSqlHook(mssql_conn_id=Variable.get('var-sqlserver-conn'))
    return sqlhook


def get_latest_maintablename(databaseid, sql_conn_id):
    get_sql = f"""
        Select 
	        TOP 1 B.cTableName,a.DatabaseID,a.ID,d.cDatabaseName,a.cDescription,a.cBuild,
            'UPDATE_' + CAST(a.DatabaseID as varchar) + '_' + CAST(a.ID as varchar) + '.zip',
            @@SERVERNAME,a.iPreviousBuildID,ISNULL((SELECT TOP 1 cBuild FROM tblBuild WHERE ID = a.iPreviousBuildID), '')
        FROM 
            tblBuild a
            INNER JOIN tblBuildTable b ON a.ID = b.BuildID
            INNER JOIN tblBuildTableLayout c ON b.ID = c.BuildTableID
            INNER JOIN tblDatabase d ON d.ID = a.DatabaseID
        WHERE 
            a.DatabaseID = {databaseid}
            AND a.iIsOnDisk = 1
            AND a.iIsReadyToUse = 1
            AND b.cTableName LIKE 'tblMain%'
            AND a.ID in (SELECT MAX(ID) FROM tblBuild WHERE iIsOnDisk = 1 AND DatabaseID = {databaseid})
    """
    logging.info('SqlServerOperator is checking credentials')
    sqlhook = MsSqlHook(mssql_conn_id=sql_conn_id)
    logging.info(f'Executing sql server query: {get_sql}')
    build_info = sqlhook.get_first(get_sql)
    if build_info:
        var_list = {"maintable_name": build_info[0],
                    'dbid': build_info[1],
                    'build_id': build_info[2],
                    'database_name': build_info[3],
                    'build_desc': build_info[4],
                    'build': build_info[5],
                    'inputfile_name': build_info[6],
                    'sqlserver_name': build_info[7],
                    'pre_buildid': build_info[8],
                    'pre_build': build_info[9],
                    }
        Variable.set(f'var-db-{str(databaseid)}', json.dumps(var_list, indent=4))

def get_top1_record (fieldnamelist, tablename, whereclause):
    strFieldnames = ','.join(fieldnamelist)
    print(str)
    sql_script = f"""
            SELECT TOP 1 <<strFieldnames>>
    	           FROM <<tablename>>
                    <<whereclause>>
        """
    sql_script = sql_script.replace('<<strFieldnames>>', strFieldnames)
    sql_script = sql_script.replace('<<tablename>>', tablename)
    sql_script = sql_script.replace('<<whereclause>>', whereclause)
    sqlhook = get_sqlserver_hook()
    logging.info(f'Executing sql server query: {sql_script}')
    sqlresult = sqlhook.get_first(sql_script)
    var_result = myDict()
    if sqlresult:
        for i in range (0, len(fieldnamelist)):
            var_result.add(fieldnamelist[i], sqlresult[i])

    return var_result
