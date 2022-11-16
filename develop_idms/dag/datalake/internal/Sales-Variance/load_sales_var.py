import os
from airflow.models import Variable
from helpers.send_email import send_email_without_config
from airflow.decorators import dag, task
from helpers.common import prepare_sql_file
from airflow.operators.python import get_current_context
from helpers.redshift import executenonquery
from datetime import datetime


default_args = {
    "owner": "data-axle",
    "start_date": datetime(2022, 11, 1),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": True,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    dag_id='datalake-internal-sales-variance',
    description="create table in redshift and load file from s3 using copy command",
    schedule_interval=None,
    max_active_runs=1,
)
def pipeline():
    @task()
    def load_data():
        # Fetch bucket, file_key from dag config
        dag_run_conf = get_current_context()["dag_run"].conf
        scriptfilename = default_args['current_path'] + '/010_load_data.sql'
        if dag_run_conf.get('bucket_name') is None or dag_run_conf.get('file_key') is None:
            raise ValueError('Please pass bucket_name,file_key argument')
        else:
            bucket_name = dag_run_conf.get('bucket_name')
            file_key = dag_run_conf.get('file_key')
        # Prepare and execute sql script
        strScript = prepare_sql_file(scriptfilename)
        # replace variable with values into sql file
        strScript = strScript.replace('{bucket_name}', bucket_name)
        strScript = strScript.replace('{file_key}', file_key)
        executenonquery(strScript)

    @task()
    def send_notification():
        send_email_without_config(
            reportname="/Reports/enterprise_sales_data_output_{yyyy}{mm}{dd}",
            dag=dag,
            email_business_users=["gary.thompson@data-axle.com"],
            email_address="mohit.khanwale@data-axle.com",
            email_subject="Airflow Notification: Loaded finance enterprise sales file to redshift",
            email_message="Successfully Copied finance enterprise sales file data from S3 to RedShift.<br/><br/> PFA sample output"
        )

    (load_data() >> send_notification())


sales_var_dag = pipeline()
