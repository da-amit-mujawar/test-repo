import os
import boto3
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
# from operators import RedshiftOperator, DataCheckOperator
from operators import DataCheckOperator
from operators.redshift import *
from helpers.send_email import send_email
from helpers.sqlserver import *


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
}


dag = DAG('z-cb-testing',
    default_args=default_args,
    description='z-cb-testing2-dag',
    tags=['test'],
    schedule_interval='@once',
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def file_directory_linecounts():
    try:
        bucket = s3.Bucket('develop_idms-2722-nua-etl-ui')
        for obj in bucket.objects.all():
            file_contents = obj.get()["Body"].read()
            print(file_contents.count('\n'))


    except Exception as e:
        logger.error("Exception: {0}".format(e))

get_filename_counts = PythonOperator(
    task_id="file_directory_linecounts",
    python_callable=file_directory_linecounts,
    # provide_context=True,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

#start_operator >> fetch_buildinfo >> data_load >> send_notification >> end_operator
start_operator >> get_filename_counts >> end_operator

