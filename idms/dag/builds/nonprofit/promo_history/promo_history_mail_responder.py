from datetime import datetime, timedelta
from email import header
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators import RedshiftOperator, DataCheckOperator
from airflow.models import Variable
from airflow.decorators import dag, task
import os
from helpers.send_email import send_email
from helpers.s3 import *
from helpers.donorbase import *
from helpers.sqlserver import get_build_info
from helpers.send_email import send_email
from helpers.common import prepare_sql_file
from airflow.operators.mssql_operator import MsSqlOperator


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 74,
    'database_type': "apogee",
    'email': [Variable.get('var-email-on-failure-datalake')],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'build_id': '24683'
}

# schedule everyday
dag = DAG('nonprofit--promo-history-load',
          default_args=default_args,
          description='load mail/responder files to redshift for nonprofit databases',
          schedule_interval='0 10 * * *',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# redshift connection
redshift_conn_id = Variable.get('var-redshift-da-rs-01-connection')
redshift_hook = PostgresHook(postgres_conn_id = redshift_conn_id)
iam = Variable.get('var-password-redshift-iam-role')

def load_data(**kwargs):
    buildid = kwargs.get("build_id")
    databaseid = kwargs.get('databaseid')
    database_type = kwargs.get('database_type')
    s3 = boto3.resource('s3')
    # s3_folder_path is f"s3://idms-7933-prod/etl/{database_type}/build-output/DW_Final_{databaseid}_{buildid}_"
    s3_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get("s3-listcoversion")
    bucket = s3.Bucket(s3_path)
    prefix = f'etl/{database_type}/build-output/DW_Final_{databaseid}_{buildid}_'
    yesterday = datetime.utcnow() - timedelta(1)
    
    def get_object(file_name):
        try:
            obj = file_name.get(IfModifiedSince=yesterday)
            return obj['Body']
        except:
            False

    # obtain a list of s3 Objects with prefix filter
    files = list(bucket.objects.filter(Prefix=prefix))
    latestfile_key = set() # {/dw_final_1438_23250_1, dw_final_1438_23250_2. dw_final_1438_23250_3}
    for file in files[0:]:
        if not (file.key.endswith("/")): # getting the file name of the S3 object
            s3_file = get_object(file) # streaming body needing to iterate through
            if s3_file:
                # meets the modified by date
                latestfile_key.add(file.key.rsplit('/',1)[0])
    for file_key in latestfile_key:
        listid = file_key.split('/')[-1].split('_')[-1]
        print(listid)

        # parquet file
        # check is data is already present
        # delete everything that is present for that listID
        sql_delete = f"""DELETE from promo_history_{databaseid} where listid={listid};"""
        redshift_hook.run(sql_delete, autocommit=True)

        strScript1 = f"""COPY datascience.PROMO_HISTORY_STAGING
                    FROM 's3://{s3_path}/{prefix}{file_key}/'
                    IAM_ROLE '{iam}'
                    FORMAT AS PARQUET;"""
        redshift_hook.run(strScript1, autocommit=True)

        strScript2 = f"""INSERT INTO datascience.PROMO_HISTORY_{databaseid} 
                        SELECT
                        Individual_ID
                        ,Family_ID
                        ,ListID
                        ,AccountNo
                        ,MailDate
                        ,Detail_DonationDate
                        ,Detail_DonationDollar
                        ,CampaignName
                        ,CampaignAudiences1
                        ,CampaignAudiences2
                        ,KeyCode
                        ,PackageCode
                        ,KeyCodeDescription
                        ,PackageCodeDescription
                        ,ListProvider
                        ,ListType
                        ,OwnerID                  
                    FROM datascience.PROMO_HISTORY_STAGING;"""

        redshift_hook.run(strScript2, autocommit=True)

        strScript3 = f"""DROP TABLE IF EXISTS datascience.PROMO_HISTORY_STAGING;"""
        redshift_hook.run(strScript3, autocommit=True)

create_table = RedshiftOperator(
    task_id='create-promo-history-table',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/010*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load_apogee = PythonOperator(
    task_id="data_load_apogee",
    python_callable=load_data,
    provide_context=True,
    op_kwargs={'databaseid': "74", 'database_type': "apogee", "build_id": "24683"},
    dag=dag)

data_load_donorbase = PythonOperator(
    task_id="data_load_donorbase",
    python_callable=load_data,
    provide_context=True,
    op_kwargs={'databaseid': "1438", 'database_type': 'donorbase', "build_id": "23250"},
    dag=dag)

# send success email
send_notification = PythonOperator(
    task_id='email_notification',
    python_callable= send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    provide_context=True,
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_table >> data_load_apogee >> data_load_donorbase >> send_notification >> end_operator
