import json
import os

import boto3 as boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import AwsGlueJobSensor
from airflow.models import Variable
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
}

dag = DAG(
    "odbx-transform-glue",
    default_args=default_args,
    description="run odbx on glue",
    schedule_interval="@once",
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
)
# config_file = os.path.abspath(os.path.dirname(__file__)) + "/config.json"
# with open(config_file) as f:
#     config = json.load(f)
config = Variable.get(
    "var-odbx-glue-settings", deserialize_json=True, default_var=None,
)

join_path = "s3://{}".format(config["bucket_name"]) + config["join_path"]
lookup_path = "s3://{}".format(config["bucket_name"]) + config["lookup_path"]
l2_path = "s3://{}".format(config["bucket_name"]) + config["l2_path"]
out_path = "s3://{}".format(config["bucket_name"]) + config["out_path"]
temp_path = "s3://{}".format(config["bucket_name"]) + config["temp_path"]
home_appr_path = "s3://{}".format(config["bucket_name"]) + config["home_appr_path"]
odb_path = "s3://{}".format(config["bucket_name"]) + config["odb_path"]
input_file_type = config["input_file_type"]
extra_py_files = "s3://{}".format(config["code_bucket"]) + config["extra_py_files"]
extra_jars_files = "s3://{}".format(config["code_bucket"]) + config["extra_jars_files"]
script_location = "s3://{}".format(config["code_bucket"]) + config["script_location"]
build_id = config["build_id"]
iam_role_name = config["iam_role_name"]
glue_job_name = config["glue_job_name"]
region_name = config["region_name"]
num_dpus = config["num_of_dpus"]
glue_version = config["glue_version"]
auto_scale = config["auto_scale"]
retry_limit = config["retry_limit"]

s3c = boto3.client("s3")
s3 = boto3.resource("s3")

odbx_files = []
odbx_bucket = config["bucket_name"]
odbx_key_prefix = config["odbx_key_prefix"]
now = datetime.now()
cur_time = now.strftime("%d%m%H%M%S")

for file in s3.Bucket(odbx_bucket).objects.filter(Prefix=odbx_key_prefix):
    s3_file = file.key.split("/")[-1]
    if s3_file != "":
        odbx_files.append(s3_file)

build_files = [s for s in odbx_files if build_id in s]

print("odbx count is ", build_files.count)

start_process = DummyOperator(task_id="Begin_Execution", dag=dag)
end_process = DummyOperator(task_id="Stop_Execution", dag=dag)

for i, path in enumerate(build_files):
    # Submit an AWS Glue job
    submit_glue_job = AwsGlueJobOperator(
        job_name=glue_job_name + "_" + path[:4] + "_" + build_id + "_" + cur_time,
        task_id=f"glue_job_{i}",
        script_location=script_location,
        s3_bucket=odbx_bucket,
        iam_role_name=iam_role_name,
        num_of_dpus=num_dpus,
        create_job_kwargs={
            "GlueVersion": glue_version,
            "Tags": {"name": "odbx", "map-migrated": "d-server-03hdamyixcl5sq"},
        },
        script_args={
            "--enable-auto-scaling": auto_scale,
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--enable-job-insights": "true",
            "--in_path": "s3://" + odbx_bucket + "/" + odbx_key_prefix + path,
            "--join_path": join_path + path[:4] + "/",
            "--lookup_path": lookup_path
            + path[:4]
            + "_ODBX49_"
            + build_id
            + "_CENSUS.gz",
            "--l2_path": l2_path,
            "--out_path": out_path + build_id + "/",
            "--odb_path": odb_path + path[:4] + "_ODB43_" + build_id + ".gz",
            "--temp_path": temp_path + path[:4] + "/",
            "--home_appr_path": home_appr_path,
            "--input_file_type": input_file_type,
            "--extra-py-files": extra_py_files,
            "--extra-jars": extra_jars_files,
        },
        region_name=region_name,
        retry_limit=retry_limit,
        dag=dag,
    )

    start_process >> submit_glue_job >> end_process
