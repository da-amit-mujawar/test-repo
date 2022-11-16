from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
import base64
import requests
import glob
import sys
import json
import os

def start_databricks_job(domain, token, jobid, jobparams):
    """
    Start Data Bricks Job (Cluster) and with runtime parameters
    """

    response = "Not Started"
    response = requests.post(f"https://{domain}/api/2.0/jobs/run-now",
        headers={
            "Authorization": b"Basic " + base64.standard_b64encode(b"token:" + token.encode("utf-8"))
        },
        json={"job_id": jobid, "timeout_seconds": "100", "notebook_params": json.loads(jobparams)}
    )

    if response.status_code != 200:
        errorstr = f"ERROR: DataBricks Returned Status for Job {jobid}. Response Code is {response.status_code}"
        logging.info(jobparams)
        logging.error(errorstr)
        raise Exception(errorstr)
    else:
        logging.info(f"DataBricks Job Started Successfully {response}")
