from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import os
import json
import boto3
from datetime import datetime
import calendar
from operators import RedshiftOperator, GenericRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 24),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False
}

dag = DAG('z-kr-business-test-dag',
          default_args=default_args,
          description='Test DAG for Business DB',
          schedule_interval='@once',
          max_active_runs=1
          )

config_file_path=default_args['currentpath'] + '/config.json'
with open(config_file_path) as f:
    config = json.load(f)

def fetchToday():
    today_date = datetime.today()
    print(today_date)
    today_formated = today_date.strftime('%Y%m%d')
    print(today_formated)
    print(datetime.today().weekday())
    day = calendar.day_name[datetime.today().weekday()]
    print(calendar.day_name[datetime.today().weekday()] )
    return today_formated, day

def fetchToday():
    today_date = datetime.today()
    #print(today_date)
    today_formated = today_date.strftime('%Y%m%d')
    #print(today_formated)
    #print(datetime.today().weekday())
    day = calendar.day_name[datetime.today().weekday()]
    #print(calendar.day_name[datetime.today().weekday()] )
    return today_formated, day

def sample_function():
    print("Sample Python Operator")

def decideLoad(ti):
    day = ti.xcom_pull(task_ids='generate_load')
    if day == "Wednesday":
        return "create_full_table"
    else:
        return "create_changes_table"

def upsert_function(**kwargs):
    print(kwargs['redshift_conn_id'])
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_drop.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    file_types = config['file_types']
    file_types_list = file_types.split(",")
    for file_type in file_types_list:
        sql2 = ""
        sql2 = sql.replace('{table}', file_type)
        redshift_hook.run(sql2)


def list_function():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('axle-gold-sources')

    for my_bucket_object in my_bucket.objects.filter(Prefix='places/full/'):
        print("Folders for full load:")
        print(my_bucket_object.key)

    for my_bucket_object in my_bucket.objects.filter(Prefix='places/changes/'):
        print("Folders for changes load:")
        print(my_bucket_object.key)

def changes_counts(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_counts.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    file_types = config['file_types']
    file_types_list = file_types.split(",")
    for file_type in file_types_list:
        sql2 = ""
        sql2 = sql.replace('{counts_table}', config.get('counts_table'))
        sql2 = sql2.replace('{file_type}', file_type)
        redshift_hook.run(sql2)

def prepare_counts_table(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_counts_table_ddl.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    sql = sql.replace('{counts_table}', config.get('counts_table'))
    redshift_hook.run(sql)


def test_function(**kwargs):
    test_value = "existing"
    print(kwargs['dag_run'].conf.get('message'))
    print(type(kwargs['dag_run'].conf.get('message')))
    if kwargs['dag_run'].conf.get('message') == None:
        test_value = "existing"
    else:
        test_value = kwargs['dag_run'].conf['message']
    print('last print ',test_value)


# test_task = PythonOperator(
#     task_id='test_task',
#     python_callable=test_function,
#     provide_context=True,
#     dag=dag,
# )
#
# drop_task = GenericRedshiftOperator(
#     task_id='drop_task',
#     dag=dag,
#     sql_file_path=default_args['currentpath'] + '/SQL/bf_drop.sql',
#     config_file_path=default_args['currentpath'] + '/config.json',
#     redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
# )

# upsert_task = PythonOperator(
#     task_id='upsert_task',
#     python_callable=upsert_function,
#     op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
#     provide_context=True,
#     dag=dag,
# )


redshift_task = RedshiftOperator(
    task_id='redshift_task_test',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/DDLtorun/bf_*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

# changes_counts_prepare = PythonOperator(
#     task_id='changes_counts_prepare',
#     python_callable=prepare_counts_table,
#     op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
#     provide_context=True,
#     dag=dag,
# )
#
# changes_counts = PythonOperator(
#     task_id='changes_counts',
#     python_callable=changes_counts,
#     op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
#     provide_context=True,
#     dag=dag,
# )

redshift_task