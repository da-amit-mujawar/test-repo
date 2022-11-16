import os
import json
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers.send_email import send_email
from helpers.s3 import *
from helpers.redshift import *
from helpers.sqlserver import *
from contextlib import closing
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 20),
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-success'),
    'email-failure': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-apogee-fec-notification',
          default_args=default_args,
          description='Email the Apogee Report Summary File',
          schedule_interval='00 02 17 * *',
          tags=['prod'],
          max_active_runs=1
          )

def rename_copy_file(**kwargs):
    copy_bucket = Variable.get("var-s3-bucket-names", deserialize_json=True)["s3-internal"]
    ls_report_file_fmt = "develop_idms/{yyyy}-{mm}-{dd}_summary.txt"
    time_stamp = (datetime.today() - timedelta(days=0)).strftime('%Y-%m-%d')
    ls_file_name = ls_report_file_fmt.replace('{yyyy}-{mm}-{dd}', time_stamp)
    ls_file_name_modified = ls_file_name[16:]
    rename_ls_file_name = f'apg_fec_{ls_file_name_modified}'
    ll_buckets = Variable.get('var-s3-bucket-names', deserialize_json=True)
    ls_bucket_name = ll_buckets['s3-apogee-fec-report']
    copy_file(ls_bucket_name, f"""{ls_bucket_name}/{ls_file_name}""", f"""develop_idms/{rename_ls_file_name}""")
    delete_file(ls_bucket_name, f"""{ls_file_name}""")

    copy_source = {
        'Bucket': ls_bucket_name,
        'Key': f'develop_idms/{rename_ls_file_name}'
    }
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(copy_bucket)
    bucket.copy(copy_source, f'Reports/{rename_ls_file_name}')


def generate_file(config_file, **kwargs):
    with open(config_file) as f:
        lj_config = json.load(f)
        ls_report_file_fmt = str(lj_config['new_report_file_name'])
        time_stamp = (datetime.today() - timedelta(days=0)).strftime('%Y-%m-%d')
        ls_file_name = ls_report_file_fmt.replace('{yyyy}-{mm}-{dd}', time_stamp)
        ll_buckets = Variable.get('var-s3-bucket-names', deserialize_json=True)
        ls_bucket_name = ll_buckets['s3-apogee-fec-report']
        ls_s3_key = 's3://' + ls_bucket_name + '/' + ls_file_name
        return ls_s3_key
        


report_file_name = generate_file(default_args['currentpath'] + '/config.json')

start_operator = DummyOperator(task_id='Begin_Execution', dag=dag)

rename_copy_text_file  = PythonOperator(
    task_id='renamed_files',
    python_callable = rename_copy_file,
    dag=dag,
)

send_notification = PythonOperator(
    task_id='Send_Notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'report_list': [report_file_name],
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_Execution', dag=dag)

start_operator >> rename_copy_text_file >> send_notification >> end_operator
