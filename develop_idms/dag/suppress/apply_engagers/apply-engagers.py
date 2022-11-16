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
from suppress.apply_engagers.tasks import *
import json
import logging

default_args = {
    'owner': 'axle-etl-cb',
    'start_date': airflow.utils.dates.days_ago(8),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

# Schedule to run Sundays at 7 pm:
dag = DAG('suppress-apply-engagers',
          default_args=default_args,
          description='apply engagers to develop_idms email dbs',
          tags=['prod'],
          schedule_interval='0 19 * * 7',
          max_active_runs=1
        )


def process_engagers(config_file, **kwargs):
    # read config file
    with open(config_file) as f:
        config = json.load(f)

    # log the details
    logging.info(f"Apply engagers to develop_idms email databases in redshift")

    # create a hook to run the stored procedure
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    # generate strScript to create trigger table
    scriptfilename = default_args['current_path'] + '/010-eng-b2b-table-create.sql'
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    # connect to server and run strScript to create table
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)
    with closing(sql_conn.cursor()) as sql_cursor:
        # get table info
        sql_cursor.execute(strScript)
        # scan through and execute.
        sql_select = f"""
                    Select databaseid, cDatabaseName, BuildID, cTablename, strsql 
                    from Tempdata.dbo.eng_b2b_tables 
                    ORDER BY databaseid
                """

        sql_cursor.execute(sql_select)
        row = sql_cursor.fetchone()
        while row:
            databaseid, cDatabaseName, BuildID, cTablename, strsql_sp = row[0], row[1], row[2], row[3],row[4]
            logging.info(f"Apply engagers on DatabaseID: {databaseid}, Database Name: {cDatabaseName},  BuildID: {BuildID}, Table name: {cTablename}")
            logging.info(f"script: {strsql_sp}")
            try:
                redshift_hook.run(strsql_sp)
                # if success, update the isApplied flag.
                sql_update = f"""
                             UPDATE Tempdata.dbo.eng_b2b_tables 
                             SET isApplied = 1
                             WHERE databaseid = {databaseid} AND ctablename = '{cTablename}'
                             """
                sqlhook.run (sql_update)
            except (Exception) as error:
                logging.info(f"ERROR: Something went for table {cTablename} and the error is {error}")

            row = sql_cursor.fetchone()

    logging.info(f"Completed.")


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
applyengagers = PythonOperator(
    task_id='applyengagers',
    python_callable=process_engagers,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
     dag=dag,
)

get_engagers_apply_report = PythonOperator(
    task_id='get_engagers_report',
    python_callable = get_engagers_counts_sql,
    op_kwargs={'dag': dag,
            'config_file': default_args['current_path'] + '/config.json'},
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> applyengagers >> get_engagers_apply_report >> end_operator
