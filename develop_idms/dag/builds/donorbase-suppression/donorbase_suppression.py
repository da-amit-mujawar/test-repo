import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator
from helpers.send_email import send_email
from helpers.s3 import *
from helpers.redshift import *
from helpers.donorbase import get_suppression_list_of_lists
from time import *
import boto3
from datetime import date, datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1438,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('donorbase-suppression',
          default_args=default_args,
          description='DONORBASE DAILY SUPPRESSIONS',
          schedule_interval='0 10 * * *',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

#redshift connection
redshift_conn_id = Variable.get('var-redshift-da-rs-01-connection')
redshift_hook = PostgresHook(postgres_conn_id = redshift_conn_id)
iam = Variable.get('var-password-redshift-iam-role')
s3_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get("s3-donorbase-silver")

def load_data(count_task_id, **kwargs):
    #read the sql file
    scriptfilename = default_args['current_path'] + '/020-load-donor-table.sql'
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    strScript = strScript.replace('{iam}', iam)
    strScript = strScript.replace('{s3-donorbase-silver}', s3_path)

    table_record_count =kwargs['ti'].xcom_pull(task_ids=count_task_id)
    if table_record_count == 0:
        file_key = 'etl/build-output/DW_Final_1438_23710_'
        listid = 0 # as table count is 0
        strScript = strScript.replace('{listid}', str(listid))
        strScript = strScript.replace('{file_key}', file_key)
        redshift_hook.run(strScript, autocommit=True)

    else:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_path)
        prefix = 'etl/build-output/DW_Final_1438_23710_'
        # note this based on UTC time
        yesterday = datetime.utcnow() - timedelta(1)
        print(yesterday)

        # function to retrieve Streaming Body from S3 with timedelta argument
        def get_object(file_name):
            try:
                obj = file_name.get(IfModifiedSince=yesterday)
                return obj['Body']
            except:
                False

        # obtain a list of s3 Objects with prefix filter
        files = list(bucket.objects.filter(Prefix=prefix))
        print(files)
        latestfile_key = set()
        for file in files[1:]:
            print(file)
            if not (file.key.endswith("/")): # getting the file name of the S3 object
                s3_file = get_object(file) # streaming body needing to iterate through
                print(s3_file)
                if s3_file: # meets the modified by date
                    latestfile_key.add(file.key.rsplit('/',1)[0])
        
        print(latestfile_key)
        for file_key in latestfile_key:
            listid = file_key.split('/')[-1].split('_')[-1]
            print(listid)
            strScript1 = f"""DELETE FROM Donorbase_AdHoc_Suppression WHERE ListID = {listid}"""
            strScript2 = f"""COPY Donorbase_AdHoc_Suppression 
                            (
                                TableID
                                ,ListID
                                ,AccountNo
                                ,FullName
                                ,FirstName
                                ,LastName
                                ,Title
                                ,Company
                                ,AddressLine1
                                ,AddressLine2
                                ,City
                                ,State
                                ,ZIP
                                ,ZIPFULL
                                ,ZIP4
                                ,SCF
                                ,Phone
                                ,EmailAddress
                                ,Gender
                                ,ListType
                                ,ProductCode
                                ,BatchDate
                                ,Individual_ID
                                ,Company_ID
                                ,DropFlag
                                ,PermissionType
                                ,ListCategory01
                                ,ListCategory02
                                ,ListCategory03
                                ,ListCategory04
                                ,ListCategory05
                                ,List_VolunteerInd
                                ,Detail_DonationDollar
                                ,Detail_DonationDate
                                ,Detail_PaymentMethod
                                ,Detail_DonationChannel
                                ,Deceased
                                ,DoNotMail
                                ,DoNotCall
                                ,RawField01
                                ,RawField02
                                ,RawField03
                                ,RawField04
                                ,RawField05
                                ,RawField06
                                ,RawField07
                                ,RawField08
                                ,RawField09
                                ,RawField10
                                ,RawField11
                                ,RawField12
                                ,RawField13
                                ,RawField14
                                ,RawField15
                                ,IndividualMC
                                ,CompanyMC
                                ,MailDate
                                ,SourceCode1
                                ,SourceCode2
                                ,SourceCode3
                                ,SourceCode4
                                ,SourceCode5
                                ,Mailabilityscore
                                ,DeceasedFlag
                            )
                            FROM 's3://{s3_path}/{file_key}/'
                            IAM_ROLE '{iam}'
                            DELIMITER '~'
                            GZIP
                            TRUNCATECOLUMNS
                            ACCEPTINVCHARS
                            TRIMBLANKS
                            MAXERROR 10000"""
            redshift_hook.run(strScript1, autocommit=True)
            redshift_hook.run(strScript2, autocommit=True)

def data_unload():
    buildid = "23710"
    # Get List of Donor Lists
    df_lol_suppression = get_suppression_list_of_lists(buildid) 
    listownerid = df_lol_suppression.groupby('OwnerID')['listid'].apply(list).to_dict()
    for ownerid in listownerid:
        sql_individual_id = ''
        sql_company_id = ''
        sql_individual_id = 'SELECT individual_id FROM Donorbase_AdHoc_Suppression WHERE listid in '+str(listownerid[ownerid]).replace('[','(').replace(']',')'+' GROUP BY individual_id')
        count_sql_individual_id = f"""SELECT COUNT(*) FROM ({sql_individual_id})"""
        count_individual_id = redshift_hook.get_records(count_sql_individual_id)[0][0]

        sql_company_id ='SELECT company_id FROM Donorbase_AdHoc_Suppression WHERE listid in '+str(listownerid[ownerid]).replace('[','(').replace(']',')'+' GROUP BY company_id')
        count_sql_company_id = f"""SELECT COUNT(*) FROM ({sql_company_id})"""
        count_company_id = redshift_hook.get_records(count_sql_company_id)[0][0]

        if count_individual_id > 0 :
            sql_unload_individualid = f"""UNLOAD ('{sql_individual_id}') 
                                    TO 's3://{s3_path}/suppression/s{ownerid}_ind.txt'
                                    IAM_ROLE '{iam}'
                                    CSV DELIMITER AS ','
                                    ALLOWOVERWRITE
                                    PARALLEL OFF"""
            redshift_hook.run(sql_unload_individualid, autocommit=True)
            copy_file(s3_path, f"""{s3_path}/suppression/s{ownerid}_ind.txt000""", f"""suppression/s{ownerid}_ind.txt""")
            delete_file(s3_path, f"""suppression/s{ownerid}_ind.txt000""")
        
        if count_company_id > 0:
            sql_unload_companyid = f"""UNLOAD ('{sql_company_id}') 
                                    TO 's3://{s3_path}/suppression/s{ownerid}_hh.txt'
                                    IAM_ROLE '{iam}'
                                    CSV DELIMITER AS ','
                                    ALLOWOVERWRITE
                                    PARALLEL OFF"""
        

            redshift_hook.run(sql_unload_companyid, autocommit=True)
            copy_file(s3_path, f"""{s3_path}/suppression/s{ownerid}_hh.txt000""", f"""suppression/s{ownerid}_hh.txt""")
            delete_file(s3_path, f"""suppression/s{ownerid}_hh.txt000""")



# run sql scripts
create_table = RedshiftOperator(
    task_id='create-table',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/010*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={'tablename': 'Donorbase_AdHoc_Suppression'},
    provide_context=True,
    dag=dag)

data_load = PythonOperator(
    task_id="data_load",
    python_callable=load_data,
    op_kwargs={"count_task_id": "count_maintable"},
    provide_context=True,
    dag=dag)

data_unload  = PythonOperator(
    task_id='data_unload',
    python_callable = data_unload,
    dag=dag,
)

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

start_operator >> create_table >> get_count >> data_load >> data_unload >> send_notification>> end_operator