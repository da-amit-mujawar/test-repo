from datetime import datetime
from operators import RedshiftOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers.send_email import send_email
from helpers.s3 import read_file
from helpers.databricks import start_databricks_job
from helpers.sqlserver import get_model_name_by_model_description
from airflow.decorators import dag, task
import logging
import base64
import requests
import glob
import sys
import json
import os
import boto3

default_args = {
    "owner": "data-axle",
    "start_date": datetime(2021, 8, 7),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": True,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    dag_id="de-databricks-model-score",
    description="Launches DataBricks Job to run model pipelines for any IDMS databases",
    schedule_interval=None,
    max_active_runs=1,
)
def pipeline():
    domain = Variable.get("var-databricks-url")
    token = Variable.get("var-databricks-secret-token")

    @task()
    def start_jobs(**kwargs):
        """
        Parse runtime args key "models" and iterate through each model's job parameters json file stored on s3
        """
        models = kwargs["dag_run"].conf.get("models", None)
        job_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get(
            "s3-datascience"
        )

        if models != None:
            # validate if the json file exists and it can be read, before starting databricks jobs
            for model in models:
                model = model.upper().strip()

                # Grab Model Name from SQL for DonorBase regression models.
                if not model.startswith("AI"):
                    logging.info(f"Fetching Regression Model Name for {model}")
                    model = get_model_name_by_model_description(model)

                if model.startswith("AI_AP"):
                    jobid = Variable.get(
                        "var-datascience", deserialize_json=True, default_var=None
                    ).get("databricks-apogee-score-job-id")
                else:
                    jobid = Variable.get(
                        "var-datascience", deserialize_json=True, default_var=None
                    ).get("databricks-donorbase-score-job-id")

                logging.info(f"Validating job for Model {model}")

                job_file = f"models/job_params/{model}.json"
                read_file(job_path, job_file)

            # Execute each Model
            for model in models:
                model = model.upper()
                logging.info(f"Starting job for Model {model}")

                job_file = f"models/job_params/{model}.json"
                logging.info(f"{job_path}{job_file}")
                payload = read_file(job_path, job_file)
                logging.info(f"Job Parameters {payload}")

                start_databricks_job(domain, token, jobid, payload)
        else:
            raise Exception("Please specify list of models to process.")

    start_jobs()


model_score_dag = pipeline()
