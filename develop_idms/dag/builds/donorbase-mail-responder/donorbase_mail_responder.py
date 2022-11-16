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
    'databaseid': 1438,
    'email': [Variable.get('var-email-on-failure-datalake')],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG('builds-donorbase-mail-responder',
          default_args=default_args,
          description='donorbase-mail-responder',
          schedule_interval='0 10 * * *',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

#redshift connection
redshift_conn_id = Variable.get('var-redshift-da-rs-01-connection')
redshift_hook = PostgresHook(postgres_conn_id = redshift_conn_id)
iam = Variable.get('var-password-redshift-iam-role')
s3_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get("s3-donorbase-silver")

def get_listid(buildid):
    sqlhook = get_sqlserver_hook()
    sql = f"""
            SELECT a.ID listid, right(a.ccode,1) ccode
                FROM DW_Admin.dbo.tblMasterLoL a
                INNER JOIN DW_Admin.dbo.tblBuildLoL b
                ON a.ID = b.MasterLoLID 
                WHERE b.BuildID = {buildid}
                AND (right(ccode,1)='M' or right(ccode,1)='R')
            """
    return sqlhook.get_pandas_df(sql)


def load_data(**kwargs):
    buildid = "23250"
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_path)
    prefix = 'etl/build-output/DW_Final_1438_23250_'
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
            if s3_file: # meets the modified by date
                latestfile_key.add(file.key.rsplit('/',1)[0])
    df_listid = get_listid(buildid)
    print(df_listid)
    logging.info(f"# of Lists {len(df_listid)}")   
    for file_key in latestfile_key:
        listid = file_key.split('/')[-1].split('_')[-1]
        print(listid)
        strScript1 = f"""COPY Donorbase_Mail_Responder 
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
                    MAXERROR 10000;"""
        redshift_hook.run(strScript1, autocommit=True)
        for index, row in df_listid.iterrows():
            sql_final_m = ''
            sql_final_r = ''
            if row["ccode"] in ('M','m') and str(row["listid"]) == str(listid):
                sql_final_m = f"""SELECT TableID,
                                    ListID,
                                    AccountNo,Individual_ID,company_id,
                                    MailDate,SourceCode1,SourceCode2,
                                    SourceCode3,SourceCode4,SourceCode5 
                                    from Donorbase_Mail_Responder where listid ={listid}"""
                unload_sql_m = f"""UNLOAD ('{sql_final_m}') 
                                        TO 's3://{s3_path}/mail_responder/{listid}.'
                                        IAM_ROLE '{iam}'
                                        DELIMITER AS '|'
                                        ALLOWOVERWRITE
                                        GZIP"""
                redshift_hook.run(unload_sql_m, autocommit=True)
                #copy_file(s3_path, f"""{s3_path}/mail_responder/{listid}.txt000""", f"""mail_responder/{listid}.txt""")
                #delete_file(s3_path, f"""mail_responder/{listid}.txt000""")
                
            
            elif row["ccode"] in ('R','r') and str(row["listid"]) == str(listid):
                sql_final_r = f"""SELECT TableID,ListID,AccountNo,Individual_ID,company_id,
                Detail_DonationDollar,Detail_DonationDate,Detail_PaymentMethod,
                Detail_DonationChannel,SourceCode1,SourceCode2,SourceCode3,SourceCode4,
                SourceCode5 from Donorbase_Mail_Responder where listid = {listid}"""
                unload_sql_r = f"""UNLOAD ('{sql_final_r}') 
                                        TO 's3://{s3_path}/mail_responder/{listid}.'
                                        IAM_ROLE '{iam}'
                                        DELIMITER AS '|'
                                        ALLOWOVERWRITE
                                        GZIP"""
                redshift_hook.run(unload_sql_r, autocommit=True)
                #copy_file(s3_path, f"""{s3_path}/mail_responder/{listid}.txt000""", f"""mail_responder/{listid}.txt""")
                #delete_file(s3_path, f"""mail_responder/{listid}.txt000""")

create_table = RedshiftOperator(
    task_id='create-table',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/010*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

drop_table = RedshiftOperator(
    task_id='drop-table',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/020-drop.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    databaseid=default_args['databaseid'],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)


data_load = PythonOperator(
    task_id="data_load",
    python_callable=load_data,
    provide_context=True,
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

start_operator >> create_table  >> data_load >> drop_table >> send_notification >> end_operator
 


