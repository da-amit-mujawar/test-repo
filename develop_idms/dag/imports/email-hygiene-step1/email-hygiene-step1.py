import json
import os
import airflow
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from helpers.send_email import send_email_without_config

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure-datalake'),
    'email_on_failure': True,
    'email_on_retry': False
}


@dag(
    dag_id='email-hygiene-step1',
    default_args=default_args,
    description='Load file from S3 to Redshift table and send unload data to oversight via AOP',
    schedule_interval='@once',
    max_active_runs=1
)
def pipeline():
    @task()
    def clean_and_load_data():
        # open config file
        config_file = default_args['current_path'] + '/config.json'
        with open(config_file) as f:
            config = json.load(f)
        # read argument value
        dag_run_conf = get_current_context()["dag_run"].conf
        if dag_run_conf.get('bucket_name') is None or dag_run_conf.get('file_key') is None:
            raise ValueError('Please pass bucket_name,file_key argument')
        else:
            bucket_name = dag_run_conf.get('bucket_name')
            file_key = dag_run_conf.get('file_key')
        # Creating redshift connection
        redshift_conn_id = Variable.get('var-redshift-da-rs-01-connection')
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        # read the sql file
        scriptfilename = default_args['current_path'] + '/010_load_data.sql'
        with open(scriptfilename, 'r') as filehandle:
            strScript = filehandle.read()
        # Extract DB Id and date from File name
        source_file_name = file_key.split('/')[-1]
        dbid = source_file_name.split('_')[1]
        source_date = source_file_name.split('_')[-1].split('.')[0]
        # replace variables with values in sql file
        strScript = strScript.replace('{bucket_name}', bucket_name)
        strScript = strScript.replace('{file_key}', file_key)
        strScript = strScript.replace('{dbid}', dbid)
        strScript = strScript.replace("{yyyy}{mm}{dd}", source_date)
        strScript = strScript.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
        strScript = strScript.replace('{kmskey}', Variable.get('var-password-kms-key'))
        config['reportname'] = config['reportname'].replace('{dbid}', dbid)
        for item in config:
            strScript = strScript.replace('{' + item + '}', config[item])
        s3_bucket_name = Variable.get('var-s3-bucket-names', deserialize_json=True)
        for bucket in s3_bucket_name:
            strScript = strScript.replace(
                '{' + bucket + '}', s3_bucket_name[bucket])
        # Execute the SQL script
        redshift_hook.run(strScript, autocommit=True)
        return dbid

    @task()
    def send_notification(dbid):
        config_file = default_args['current_path'] + '/config.json'
        with open(config_file) as f:
            config = json.load(f)
        # Update reportname with dbid
        config['reportname'] = config['reportname'].replace('{dbid}', dbid)
        config['email_subject'] = config['email_subject'].replace('{dbid}', dbid)
        # send success email with report as an attachment
        send_email_without_config(
            reportname=config['reportname'],
            dag=dag,
            email_address=config['email_address'],
            email_subject=config['email_subject'],
            email_message=config['email_message']
        )

    (send_notification(clean_and_load_data()))


ehyg_s1_dag = pipeline()
