import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.redshift import get_redshift_hook
import pandas as pd
import numpy as np
import io
import boto3
import json
import logging
import glob

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}



# Schedule to run daily after 4:30.  check VC job that exports from 3:45 am
#schedule to run 6 am
dag = DAG('imports-optout-hardbounce-suppressions',
          default_args=default_args,
          description='imports optout-hardbounce suppression lookup',
          schedule_interval='0 9 * * *',
          max_active_runs=1
          )

def insert_oohh_table(config_file,  **kwargs):
    #read config file
    with open(config_file) as f:
        config = json.load(f)

    s3key = config['s3-key1']
    listconversionbucketname = config['listconversionbucket']
    filename_activeb2b =config["filename-activeb2b"]

    #get list of bucket names from variable. need it later
    s3_bucket_name = Variable.get('var-s3-bucket-names', deserialize_json=True)

    #replace  the bucket name for filename_activeb2b
    for bucket in s3_bucket_name:
        listconversionbucketname = listconversionbucketname.replace('{' + bucket + '}', s3_bucket_name[bucket])

    #log the details
    logging.info(f"List of active b2b filename {filename_activeb2b}")

    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=listconversionbucketname, Key=filename_activeb2b)
    dfPandas = pd.read_csv((io.BytesIO(obj['Body'].read())), encoding='unicode_escape', delimiter=',',
                           error_bad_lines=False, low_memory=False)
    dfPandas.columns = dfPandas.columns.str.upper()
    databaselist = dfPandas["DATABASEID"].tolist()

    # read the script file
    scriptfilename = default_args['current_path'] + '/010-optout-hardbounce-suppressions-loadtable.sql'
    filehandle = open(scriptfilename, 'r')
    strScript_master = filehandle.read()
    filehandle.close()

    #replace the constant varialbes except the databaseid
    strScript_master = strScript_master.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
    strScript_master = strScript_master.replace('{kmskey}', Variable.get('var-password-kms-key'))
    for item in config:
        strScript_master = strScript_master.replace('{' + item + '}', config[item])

    for bucket in s3_bucket_name:
        strScript_master = strScript_master.replace('{' + bucket + '}', s3_bucket_name[bucket])

    logging.info(f"script:  {strScript_master}")

    #create a hook to run the sql
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)

    for i in range(len(databaselist)):
        databaseid = str(databaselist[i])
        logging.info(f"Insert and Update optouts & hardbounce for database:  {databaseid}")

        #do the replace the database id in the script
        strScript = strScript_master
        strScript = strScript.replace('{DATABASEID}', databaseid)

        #Ready. now do the work. don't use Try catch
        redshift_hook.run(strScript)

    #finally show the delete count
    logging.info(f"Completed.")




start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
data_load = PythonOperator(
    task_id='loading-optout-hardbounce-suppressions',
    python_callable=insert_oohh_table,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
     dag=dag,
)

# # check counts
# data_check = DataCheckOperator(
#     task_id='validating-record-count',
#     dag=dag,
#     config_file_path=default_args['current_path'] + '/config.json',
#     min_expected_count=[
#                             ('tablename1', 200)
#                         ],
#     # redshift_conn_id=Variable.get('var-redshift-jdbc-conn')
#     redshift_conn_id=Variable.get('var-redshift-postgres-conn')
# )


send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> data_load >> send_notification >> end_operator
