import os
import json
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers.send_email import send_email
from helpers.s3 import *
from helpers.redshift import *
from helpers.sqlserver import *
from contextlib import closing


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 881,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}
# triggered with lambda when file appears
dag = DAG('builds-cobr-weekly',
          default_args=default_args,
          description='builds cobr weekly',
          schedule_interval='@once',
          tags=['dev'],
          max_active_runs=1
          )

def update_sql_file(config_file,sql_file_name,**kwargs):
    #Checking for redshift connection
    redshift_conn_id = Variable.get('var-redshift-postgres-conn')
    redshift_hook = PostgresHook(postgres_conn_id = redshift_conn_id)

    #read the sql file
    scriptfilename = default_args['current_path'] + sql_file_name
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    #open config file
    with open(config_file) as f:
        config = json.load(f)

    # read argument value
    if kwargs['dag_run'].conf.get('bucket_name') == None or kwargs['dag_run'].conf.get('file_key') == None:
        raise ValueError('Please pass bucket_name,file_key argument')
    else:
        bucket_name = kwargs['dag_run'].conf.get('bucket_name')
        file_key = kwargs['dag_run'].conf.get('file_key')

    #replace variable with values into sql file 
    strScript = strScript.replace('{bucket_name}', bucket_name)
    strScript = strScript.replace('{filename}', file_key)
    strScript = strScript.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
    
    for item in config:
        strScript = strScript.replace('{' + item + '}', config[item])
        
    redshift_hook.run(strScript, autocommit=True)

def rename_text_file(**kwargs):
    output_bucket_name = Variable.get("var-s3-bucket-names", deserialize_json=True)["s3-internal"]
    file_name = kwargs['dag_run'].conf.get('file_key')
    idx = file_name.index('COBR')
    output_key = file_name[:idx] + 'Processed.' + file_name[idx:]
    file_path_old = f"""{output_bucket_name}/{file_name}"""
    copy_file(output_bucket_name, file_path_old, output_key)
    delete_file(output_bucket_name, file_name)

def activate_the_build_sql(count_task_id,**kwargs):
    file_name = kwargs['dag_run'].conf.get('file_key')
    table_record_count =kwargs['ti'].xcom_pull(task_ids=count_task_id)
    sqlhook = get_sqlserver_hook()
    sql_conn = sqlhook.get_conn()
    sql_conn.autocommit(True)

    with closing(sql_conn.cursor()) as sql_cursor:
        sql_script = f"""
        BEGIN
        DECLARE @DayOfweek  Varchar(20);
        DECLARE @BuildId  int;

        --select @DayOfweek = left(upper(DATENAME(weekday,GETDATE())),3);
        select @DayOfweek = LEFT(RIGHT('{file_name}', LEN('{file_name}')-26), 3);

        SELECT @BuildId = CASE @DayOfweek 
        WHEN 'WED' THEN 14112 
        WHEN 'FRI' THEN 14111 
        ELSE -1
        END;

        UPDATE TOP (1)  DW_Admin.dbo.tblBuild 
        SET LK_BuildStatus='61', dMailDate=GETDATE(), dScheduledDateTime = GETDATE(), 
        iRecordCount = {table_record_count}
        WHERE ID = @BuildId;
        END;
        """
        print(sql_script)
        sql_cursor.execute(sql_script)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
data_load1 = PythonOperator(
    task_id='create_cobr_initial',
    python_callable = update_sql_file,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'sql_file_name': '/010-create-cobr-initial.sql'
               },
    dag=dag
)

data_load2 = RedshiftOperator(
    task_id='create_cobr_final',
    dag=dag,
    sql_file_path=default_args['current_path'] + '/02*.sql',
    config_file_path=default_args['current_path'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load3 = PythonOperator(
    task_id='rename_table',
    python_callable = update_sql_file,
    dag=dag,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'sql_file_name': '/013-rename.sql'
               }
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={'tablename': 'tblCobraWeekly881_Final'},
    provide_context=True,
    dag=dag)

activate_the_build_sql = PythonOperator(
    task_id='activate_the_build_sql',
    python_callable=activate_the_build_sql,
    op_kwargs={
        "count_task_id": "count_maintable"
    },
    provide_context=True,
    dag=dag,
)

rename_text_file = PythonOperator(
    task_id='rename_text_file',
    python_callable=rename_text_file,
    dag=dag,
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> data_load1 >> data_load2 >> get_count >> data_load3 >> activate_the_build_sql >> rename_text_file >>send_notification>> end_operator
