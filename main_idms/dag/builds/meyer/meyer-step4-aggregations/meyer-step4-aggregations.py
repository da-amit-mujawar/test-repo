import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from operators import RedshiftOperator, DataCheckOperator
from operators.redshift import *
from helpers.send_email import send_email, send_email_without_config
from helpers.sqlserver import *

default_args = {
    'owner': 'axle-etl-cb',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1352,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

time_stamp = datetime.today().strftime("%Y%m%d")

dag = DAG('builds-meyer-step4-aggregations',
          default_args=default_args,
          description='builds meyer',
          schedule_interval='@once',
          tags=['prod'],
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# get build
fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info_plus,
    op_kwargs={'databaseid': default_args['databaseid'],
               'active_flag': 0,  # 0 or 1
               'latest_maintable_dbid': 1267 }, #0 if not needed
    provide_context=True,
    dag=dag)

#run load scripts
data_load = RedshiftOperator(
    task_id='loading_child_tables',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/0*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# check counts
data_check = DataCheckOperator(
    task_id='validating-record-count',
    dag=dag,
    config_file_path=default_args['current_path'] + '/config.json',
    min_expected_count=[('new_school', 80),
                        ('new_rate_chart', 50),
                        ('new_first_degree', 1500),
                        ('new_generic_salutation', 10),
                        ('new_state_availability', 50),
                        ('new_title_standardization', 400)
                        ],
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# run aggregation scripts
data_aggregations = RedshiftOperator(
    task_id='tblmain_aggregations_and_suppressions',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/3*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# rename tables and generate output
activate = RedshiftOperator(
    task_id='finalize-tables-generate-reports',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/4*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

# # send success email
# send_notification = PythonOperator(
#     task_id='email_notification',
#     python_callable=send_email,
#     op_kwargs={'config_file': default_args['current_path'] + '/config.json',
#                'dag': dag
#                },
#     dag=dag)

# send success email
send_email_without_config(
    report_list=[f"/tmp/meyer_aggregations_summary_listid_{time_stamp}000.gz",
                 f"/tmp/meyer_aggregations_summary_{time_stamp}000.gz",
                 f"/tmp/meyer_aggregations_product_summary_{time_stamp}000.gz"],
    email_business_users=["DL-MeyerServiceTeam@data-axle.com"],
    email_subject=f"MEYER Aggregations Step 4 Completed",
    email_message="Hello IDMS-Admin Team,</br></br> Meyer Aggregations Step 4 process complete. Please find attached audit reports.")


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# start_operator >> fetch_buildinfo >> data_load >> data_check >> data_aggregations >> activate >> send_notification >> end_operator
start_operator >> fetch_buildinfo >> data_load >> data_check >> data_aggregations >> activate >> send_email_without_config >> end_operator
