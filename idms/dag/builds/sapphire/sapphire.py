import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from operators import RedshiftOperator, DataCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mssql_operator import MsSqlOperator
from helpers.send_email import send_email
from helpers.sqlserver import *
from helpers.redshift import *

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 71,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-sapphire-redshift-load',
          default_args=default_args,
          description='builds-sapphire-redshift-load',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

fetch_buildinfo = PythonOperator(
    task_id="fetch_build",
    python_callable=get_build_info,
    op_kwargs={'databaseid': default_args['databaseid']
               },
    provide_context=True,
    dag=dag)
config = Variable.get('var-db-71', deserialize_json=True)
build_id = config['build_id']
report_path = f'tmp/{build_id}_Postal_Email_Counts'

# run sql scripts
data_load = RedshiftOperator(
    task_id='data_load',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/01*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

#run updates
data_update_1 = RedshiftOperator(
    task_id='data_update_1',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/030*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_update_2 = RedshiftOperator(
    task_id='data_update_2',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/031*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_update_3 = RedshiftOperator(
    task_id='data_update_3',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/032*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_count = RedshiftOperator(
    task_id='data_count',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/033*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

def get_output_count():
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    postal_email_count_script = """SELECT
     Col1,
     Col2,
     Col3,
     Col4,
     Col5
    FROM SapphireBuildCounts_ToBeDropped A
    Order by ID asc ;"""

    orders_df = redshift_hook.get_pandas_df(postal_email_count_script)
    print(orders_df.to_markdown())
    orders_df.to_csv(report_path, ',', index=False, header=False)

get_sapphire_postal_email_counts = PythonOperator(
    task_id='get_sapphire_postal_email_counts',
    python_callable=get_output_count,
    op_kwargs={'build_id': build_id,
                'report_path': report_path},
    dag=dag,
)


send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'report_list': [report_path],
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

(
    start_operator
    >> fetch_buildinfo
    >> data_load
    >> data_update_1
    >> data_update_2
    >> data_update_3
    >> data_count
    >> get_sapphire_postal_email_counts
    >> send_notification
    >> end_operator
)