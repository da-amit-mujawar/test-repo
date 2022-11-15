import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.redshift import get_redshift_hook
from helpers.sqlserver import get_sqlserver_hook
from contextlib import closing
from suppress.apply_donotcall.tasks import *
import json
import logging


default_args = {
    'owner': 'axle-etl-cb',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('suppress-apply-donotcall',
          default_args=default_args,
          description='suppress-apply-do-not-call',
          tags=['prod'],
          schedule_interval='0 6 * * *',
          max_active_runs=1
          )

def apply_donotcall_script(config_file,  **kwargs):
    #read config file
    with open(config_file) as f:
        config = json.load(f)

    #log the details
    logging.info(f"Apply Do Not Call on B2C databases in Redshift")

    #create a hook to run the stored procedure sp_suppress_oohb
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    # read the script file
    scriptfilename = default_args['current_path'] + '/010-get-donotcall-update-script.sql'
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    logging.info(f"script:  {strScript}")

    #get sql connection and run the sql to re-create Tempdata.dbo.dnc_b2c_tables
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)
    with closing(sql_conn.cursor()) as sql_cursor:

        #recreate Tempdata.dbo.dnc_b2c_tables
        sql_cursor.execute(strScript)
        # scan through and execute.
        sql_select = f"""
                    Select databaseid, cDatabaseName, BuildID, cTablename, strsql 
                    from Tempdata.dbo.dnc_b2c_tables 
                    where isaws =1
                    ORDER BY databaseid
                """

        sql_cursor.execute(sql_select)
        row = sql_cursor.fetchone()
        while row:
            databaseid, cDatabaseName, BuildID, cTablename, strsql_sp = row[0], row[1], row[2], row[3],row[4]
            logging.info(f"Apply DNC flag on DatabaseID: {databaseid}, Database Name: {cDatabaseName},  BuildID: {BuildID}, Table name: {cTablename}")
            logging.info(f"script: {strsql_sp}")
            #if error, write to log and continue.  don't stop.
            try:
                redshift_hook.run(strsql_sp)
                #if success, update the isApplied flag.
                sql_update = f"""
                             UPDATE Tempdata.dbo.dnc_b2c_tables SET isApplied = 1
                             WHERE isaws =1 AND databaseid = {databaseid} AND ctablename = '{cTablename}'
                             """
                sqlhook.run (sql_update)
            except:
                logging.info(f"Unable to apply DNC flags. Make sure the ctablename available in Redshift.  Tablename: {cTablename}")
            row = sql_cursor.fetchone()

    logging.info(f"Completed.")


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

applydonotcall = PythonOperator(
    task_id='applying-donotcall-suppressions',
    python_callable=apply_donotcall_script,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
     dag=dag,
)


get_donotcall_apply_report = PythonOperator(
    task_id='get_donotcall_apply_report',
    python_callable=get_donotcall_apply_counts_sql,
    op_kwargs={'dag': dag,
            'config_file': default_args['current_path'] + '/config.json'},
    dag=dag,
)


send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> applydonotcall >> get_donotcall_apply_report >> send_notification >> end_operator
