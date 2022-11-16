import json
import os
import boto3 as boto3
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import (
    AwsGlueJobOperator,
)
from airflow.providers.amazon.aws.sensors.glue import AwsGlueJobSensor
from airflow.models import Variable
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
}

dag = DAG(
    "nonprofit-dataprep-glue",
    default_args=default_args,
    description="run np-dataprep on glue",
    schedule_interval="@daily",
    max_active_runs=1,
)

# config_file = os.path.abspath(os.path.dirname(__file__)) + "/config.json"
# with open(config_file) as f:
#     config = json.load(f)

config = Variable.get(
    "var-dataprep-glue-settings",
    deserialize_json=True,
    default_var=None,
)

# TODO fetch form Promo history
client = "ALCA"  # client name
listid = '19348'  # unique ID in Donorbase validation report
maildates = ["2016-10-25", "2017-10-24"]  # mail dates for each file.
mail_files = ['21407.txt',  # 330 ALICE LLOYD COLLEGE - REG MAILER 1
              '21408.txt']  # 330 ALICE LLOYD COLLEGE - REG MAILER 2
response_files = ["21409.txt", "21094.txt"]


bucket_name = config["bucket_name"] #"axle-donorbase-silver-sources"
s3c = boto3.client("s3")
s3 = boto3.resource("s3")
donorbase_s3_folder = f"s3://{bucket_name}/exports/transactions/{listid}/"
mail_responce_files_s3_folder = f"s3://{bucket_name}/mail_responder/"
response_file_s3_paths = [f"{mail_responce_files_s3_folder}{file_name}.txt" for file_name in response_files]
output_s3_folder = f"donorbase-clients/{client}/{(datetime.today()).strftime('%Y%m%d%H%M%S')}/response/raw/"
output_file_name = "response.csv"

def check_default(key: str, default):
    if key in config:
        return config[key]
    else:
        return default

extra_py_files = "s3://{}".format(config["bucket_name"]) + config["extra_py_files"]
extra_jars_files = "s3://{}".format(config["bucket_name"]) + config["extra_jars_files"]
script_location = "s3://{}".format(config["bucket_name"]) + config["script_location"]
iam_role_name = config["iam_role_name"]
glue_job_name = config["glue_job_name"]
region_name = config["region_name"]
glue_version = config["glue_version"]
num_dpus = check_default("num_of_dpus", 3)
auto_scale = check_default("auto_scale", False)
retry_limit = check_default("retry_limit", 1)

#
# for file in s3.Bucket(odbx_bucket).objects.filter(Prefix=odbx_key_prefix):
#     s3_file = file.key.split("/")[-1]
#     if s3_file != "":
#         odbx_files.append(s3_file)
#
# print("odbx count is ", odbx_files.count)

start_process = DummyOperator(task_id="Begin_Execution", dag=dag)
end_process = DummyOperator(task_id="Stop_Execution", dag=dag)

submit_glue_job = AwsGlueJobOperator(
    job_name=glue_job_name + "_" + listid,
    task_id=f"glue_job_{listid}",
    script_location=script_location,
    # TODO check if this on working
    # script_location=os.path.abspath(os.path.dirname(__file__)) + "/np_dataprep.py",
    s3_bucket=bucket_name,
    iam_role_name=iam_role_name,
    num_of_dpus=num_dpus,
    create_job_kwargs={
        "GlueVersion": glue_version,
    },
    script_args={
        "--enable-auto-scaling": auto_scale,
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--enable-job-insights": "true",
        # "--in_path": "s3://" + bucket + "/" + odbx_key_prefix + "/" + path,
        # "--join_path": join_path + path + "/",
        # "--lookup_path": lookup_path,
        # "--l2_path": l2_path,
        # "--out_path": out_path,
        # "--temp_path": temp_path + path + "/",
        "--extra-py-files": extra_py_files,
        "--extra-jars": extra_jars_files,
        "--tags": "name=odbx",
    },
    region_name=region_name,
    retry_limit=retry_limit,
    dag=dag,
)

start_process >> submit_glue_job >> end_process
