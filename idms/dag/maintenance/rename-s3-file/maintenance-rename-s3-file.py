import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "data-axle",
    "start_date": datetime(2022, 7, 25),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": True,
    "email_on_retry": False,
}


def rename_s3_object(bucket, source_path, target_path):
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, target_path).copy_from(
        CopySource=f"{bucket}/{source_path}"
    )
    s3_resource.Object(bucket, source_path).delete()


dag = DAG(
    default_args=default_args,
    dag_id="maintenance-rename-s3-file",
    description="Rename an s3 object",
    schedule_interval=None,
    max_active_runs=1,
)

start = DummyOperator(task_id="Begin_execution", dag=dag)

s3_rename_task = PythonOperator(
    task_id="s3_rename_task",
    python_callable=rename_s3_object,
    op_kwargs={
        "bucket": "{{ dag_run.conf['bucket'] }}",
        "source_path": "{{ dag_run.conf['source_path'] }}",
        "target_path": "{{ dag_run.conf['target_path'] }}",
    },
    dag=dag,
)

end = DummyOperator(task_id="Finish_execution", dag=dag)

start >> s3_rename_task >> end
