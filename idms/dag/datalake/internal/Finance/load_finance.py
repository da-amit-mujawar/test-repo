import json
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers.send_email import send_email


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure-datalake'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('datalake-internal-finance',
          default_args=default_args,
          description='create table in redshift and load file from s3 using copy command',
          schedule_interval='@once',
          max_active_runs=1
          )

def update_sql_file(config_file,**kwargs):
    #Checking for redshift connection
    redshift_conn_id = Variable.get('var-redshift-da-rs-01-connection')
    redshift_hook = PostgresHook(postgres_conn_id = redshift_conn_id)

    #read the sql file
    scriptfilename = default_args['current_path'] +'/010_load_data.sql'
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    #open config file
    with open(config_file) as f:
        config = json.load(f)

    # read argument value
    if kwargs['dag_run'].conf.get('bucket_name') == None or kwargs['dag_run'].conf.get('file_key') == None:
        raise ValueError('Please pass bucket_name,file_key argument')
    else:
        bucket_name = kwargs['dag_run'].conf.get('bucket_name')
        file_key = kwargs['dag_run'].conf.get('file_key')

    #replace variable with values into sql file 
    strScript = strScript.replace('{bucket_name}', bucket_name)
    strScript = strScript.replace('{file_key}', file_key)
    strScript = strScript.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
    strScript = strScript.replace('{kmskey}', Variable.get('var-password-kms-key'))
    
    for item in config:
        strScript = strScript.replace('{' + item + '}', config[item])

    s3_bucket_name = Variable.get('var-s3-bucket-names', deserialize_json=True)
    for bucket in s3_bucket_name:
        strScript = strScript.replace(
            '{' + bucket + '}', s3_bucket_name[bucket])

    redshift_hook.run(strScript, autocommit=True)


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

data_load = PythonOperator(
    task_id='data_load',
    python_callable=update_sql_file,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag
)

# send success email
send_notification = PythonOperator(
    task_id='email_notification',
    python_callable= send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    provide_context=True,
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> data_load >> send_notification >> end_operator