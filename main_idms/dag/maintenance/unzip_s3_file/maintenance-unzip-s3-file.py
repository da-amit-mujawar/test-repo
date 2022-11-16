import os
import logging
import zipfile
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from maintenance.unzip_s3_file.unzip_util import S3ZipFile

DEFAULT_BUFFER_SIZE = 100 * 1024 * 1024

default_args = {
    "owner": "data-axle",
    "start_date": datetime(2022, 7, 25),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": True,
    "email_on_retry": False,
}


def unzip_file(bucket, zip_file_path, target_path):
    logging.info("Unzipping bundle and extracting to s3 folder!")
    try:
        with S3ZipFile(bucket, zip_file_path, DEFAULT_BUFFER_SIZE) as zip_ref:
            zip_ref.open()
            for filename in zip_ref.get_filenames():
                zip_ref.extract(filename, target_path)
                logging.info(f"Extracted {filename} to s3 folder {target_path}")

    except zipfile.BadZipFile as bzf:
        message = f"Error while opening zip file: {zip_file_path}, errorMessage:{bzf}"
        raise Exception(bzf, message)

    logging.info(
        f" Zipfile {zip_file_path} successfully unzipped to s3 folder {target_path}"
    )


dag = DAG(
    default_args=default_args,
    dag_id="maintenance-unzip-s3-file",
    description="Unzip S3 file",
    schedule_interval=None,
    max_active_runs=1,
)

start = DummyOperator(task_id="Begin_execution", dag=dag)

unzip_task = PythonOperator(
    task_id="unzip_task",
    python_callable=unzip_file,
    op_kwargs={
        "bucket": "{{ dag_run.conf['bucket'] }}",
        "zip_file_path": "{{ dag_run.conf['file_path'] }}",
        "target_path": "{{ dag_run.conf['target_location'] }}",
    },
    dag=dag,
)

end = DummyOperator(task_id="Finish_execution", dag=dag)

start >> unzip_task >> end
