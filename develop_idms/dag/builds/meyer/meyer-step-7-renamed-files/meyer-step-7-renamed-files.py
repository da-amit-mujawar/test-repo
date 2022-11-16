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

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-meyer-step7-renamed-files',
          default_args=default_args,
          description='builds-meyer-step7-renamed-files',
          tags=['prod'],
          schedule_interval='@once',
          max_active_runs=1
          )


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

def rename_text_file(**kwargs):
    old_bucket_name = Variable.get("var-s3-bucket-names", deserialize_json=True)["s3-aopoutput"]
    new_bucket_name = 'axle-customer-meyer'
    new_prefix = 'mcd/'
    s3 = boto3.resource('s3')
    old_bucket = s3.Bucket(old_bucket_name)
    new_bucket = s3.Bucket(new_bucket_name)
    files = list(old_bucket.objects.filter(Prefix=''))
    date_id = (datetime.today()).strftime('%Y%m%d')
    for file in files[1:]:
        file_name = file.key
        if file_name.startswith("a04"):
            copy_file(old_bucket_name, f"""{old_bucket_name}/{file_name}""", f"""processed/{file_name}""")
            delete_file(old_bucket_name, f"""{file_name}""")
    files_new = list(old_bucket.objects.filter(Prefix='',Delimiter='/'))
    for file in files_new[1:]:
        file_name = file.key
        if file_name.startswith("a05") or file_name.startswith("A07"):
            index_of_dot = file_name.rfind('.')
            file_name_without_extension = file_name[:index_of_dot]
            file_name_suffix = f'{file_name_without_extension}_{date_id}'
            copy_file(old_bucket_name, f"""{old_bucket_name}/{file_name}""", f"""processed/{file_name_suffix}""")
            delete_file(old_bucket_name, f"""{file_name}""")
    files_processed = list(old_bucket.objects.filter(Prefix='processed/',Delimiter='/'))
    for file in files_processed[1:]:
        file_name = file.key
        print(file_name)
        file_modified = file_name[10:]
        if file_modified == f"a05_meyers_xref_{date_id}" or file_modified == f"a05_meyers_work_matched_{date_id}":
                old_source = { 'Bucket': old_bucket_name, 'Key': f"processed/{file_modified}"} # replace the prefix
                new_key = file_name.replace('processed/', new_prefix, 1)
                new_obj = new_bucket.Object(new_key)
                new_obj.copy(old_source)
            
            
                   
rename_text_file  = PythonOperator(
    task_id='renamed_files',
    python_callable = rename_text_file,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> rename_text_file >> end_operator