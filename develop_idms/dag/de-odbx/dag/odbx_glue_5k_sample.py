import json
import os

import boto3 as boto3
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import AwsGlueJobSensor
from airflow.models import Variable
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
}

dag = DAG(
    "odbx-generate-sample",
    default_args=default_args,
    description="generate 5k/1million sample for odbx",
    schedule_interval="@once",
    max_active_runs=1,
)

config = Variable.get(
    "var-odbx-generate-sample-settings", deserialize_json=True, default_var=None,
)

odbx_path = "s3://{}".format(config["bucket_name"]) + config["odbx_path"]
odb_path = "s3://{}".format(config["bucket_name"]) + config["odb_path"]
out_path = "s3://{}".format(config["bucket_name"]) + config["out_path"]
lookup_path = "s3://{}".format(config["bucket_name"]) + config["lookup_path"]
l2_path = "s3://{}".format(config["bucket_name"]) + config["l2_path"]
home_appr_path = "s3://{}".format(config["bucket_name"]) + config["home_appr_path"]
input_file_type = config["input_file_type"]
script_location = "s3://{}".format(config["code_bucket"]) + config["script_location"]
extra_jars_files = "s3://{}".format(config["code_bucket"]) + config["extra_jars_files"]
iam_role_name = config["iam_role_name"]
glue_job_name = config["glue_job_name"]
region_name = config["region_name"]
num_dpus = config["num_of_dpus"]
glue_version = config["glue_version"]
auto_scale = config["auto_scale"]
retry_limit = config["retry_limit"]
odbx_bucket = config["bucket_name"]

start_process = DummyOperator(task_id="Begin_Execution", dag=dag)
end_process = DummyOperator(task_id="Stop_Execution", dag=dag)

submit_glue_job = AwsGlueJobOperator(
    job_name=glue_job_name,
    task_id="odbx_sample_data_step",
    script_location=script_location,
    s3_bucket=odbx_bucket,
    iam_role_name=iam_role_name,
    num_of_dpus=num_dpus,
    create_job_kwargs={"GlueVersion": glue_version},
    script_args={
        "--enable-auto-scaling": auto_scale,
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--enable-job-insights": "true",
        "--odbx_path": odbx_path,
        "--odb_path": odb_path,
        "--out_path": out_path,
        "--lookup_path": lookup_path,
        "--l2_path": l2_path,
        "--home_appr_path": home_appr_path,
        "--input_file_type": input_file_type,
        "--extra-jars": extra_jars_files,
        "--tags": "name=odbx,map-migrated=d-server-03hdamyixcl5sq",
    },
    region_name=region_name,
    retry_limit=retry_limit,
    dag=dag,
)

start_process >> submit_glue_job >> end_process
