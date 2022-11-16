import os
import pandas as pd
from os.path import dirname, abspath
from pyparsing import Or
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator
from helpers.send_email import send_email
from helpers.s3 import *
from helpers.sqlserver import *

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

dag = DAG('builds-donorbase-opu',
          default_args=default_args,
          description='donorbase-opu',
          schedule_interval='@once',
          max_active_runs=1
          )

redshift_conn_id = Variable.get('var-redshift-postgres-conn')
redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
s3_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get("s3-aopinput")



def update_sql_file(**kwargs):
    scriptfilename = default_args['current_path'] +"/010-create-copy.sql"
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    # read argument value
    if kwargs['dag_run'].conf.get('file_key') == None:
        raise ValueError('Please pass filekey')
    else:
        file_key1 = kwargs['dag_run'].conf.get('file_key')
        file_key = file_key1.replace("+"," ") 
    
    strScript = strScript.replace('{filename}', file_key)
    strScript = strScript.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
    
    redshift_hook.run(strScript, autocommit=True)
    
def get_output_format():
    
    sql = f"""select trim(regexp_substr(col001,'[^:]+$')) as op_format from templayout where substring(col001,1,13) = 'Output Format'"""
    
    record_output = redshift_hook.get_records(sql)
    return record_output[0][0]
    
def update_sql_file_output(fetch_result,**kwargs):
    # read the sql file
    scriptfilename = default_args['current_path'] +"/011-1-import-layout-fixed.sql"
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()
    file_key1 = kwargs['dag_run'].conf.get('file_key')
    file_key = file_key1.replace("+"," ") 
    layout_file_name = file_key.split("/")[-1]
    table_record_result =kwargs['ti'].xcom_pull(task_ids=fetch_result)
    order_id = ''.join(filter(str.isdigit, file_key))
    crte_stmt = []
    crte_stmt = ' create table donorb_{order_id}_order ( '
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('axle-donorbase-raw-sources')
    body = ''
    y = 0
    # Iterates through all the objects, Each obj 
    # is an ObjectSummary, so it doesn't contain the body. You'll need to call get to get the whole body.
    for obj in bucket.objects.filter(Prefix=file_key):
        body = obj.get()['Body'].readlines()
        for line in body:
            a = line.decode("utf-8")
            x = a[0:7]
            if x == "Field #":
                y = 1
            if y == 1 and a != '\r\n':
                column_list = a[9:25].rstrip()
                column_list_modified = column_list.replace(' ', '_')
                crte_stmt = crte_stmt + ' ' + '[' + column_list_modified + ']'
                crte_stmt = crte_stmt + ' ' + 'varchar(' + a[72:75].rstrip() + '),'
            if y == 1 and a == '\r\n':
                y = 0
                crte_stmt = crte_stmt + ')'
                crte_stmt = crte_stmt.replace('),)', ') )')
                crte_stmt_beg=crte_stmt.split('(')[0]+'('
                crte_stmt_end=crte_stmt[67:]
                crte_stmt = crte_stmt_beg+crte_stmt_end
                break
        
        
    # read argument value
   
    if table_record_result.upper() == 'FIXED':
        fixedfilename = ''
        if layout_file_name == str(order_id)+".layout"+".txt":
            fixedfilename = "{order_id}.order.txt"
        elif  layout_file_name == "Layout." +str(order_id)+".txt":
            fixedfilename = "Order.{order_id}.Fixed.txt"
        
        copy_command = []
        copy_command = r"""COPY donorb_{order_id}_order FROM ''s3://axle-donorbase-raw-sources/Prior LiftEngine Orders/"""+str(fixedfilename)+r"""'' iam_role ''{iam}'' fixedwidth '''"""
        y = 0
        for obj in bucket.objects.filter(Prefix=file_key):
            body = obj.get()['Body'].readlines()
        for line in body:  
            a = line.decode("utf-8")
            x = a[0:7]
            if x == "Field #":
                y = 1
            if y == 1 and a != '\r\n':
                column_list = a[9:25].rstrip()
                column_list_modified = column_list.replace(' ', '_')
                copy_command = copy_command + ' ' + column_list_modified 
                copy_command = copy_command + ' ' + ':'+a[72:75].rstrip() + ','
            if y == 1 and a == '\r\n':
                y = 0
                copy_command=copy_command[:-1]
                copy_command="".join((copy_command,r"""''"""))
                copy_command_beg = copy_command.split(',')[0][:-17]
                copy_command_mid = copy_command.split('fixedwidth')[-1]
                copy_command_end = copy_command_mid.split(',')[1:]
                listToStr = ','.join([str(elem) for elem in copy_command_end])
                copy_command =  copy_command_beg + listToStr
                break
        
        strScript = strScript.replace('{crte_stmt}', str(crte_stmt))
        strScript = strScript.replace('{copy_command}', str(copy_command))
        strScript = strScript.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
        strScript = strScript.replace('{s3-aopinput}', s3_path)
        # strScript = strScript.replace('{fixedfilename}', str(fixedfilename))
        strScript = strScript.replace('{order_id}', order_id)

        redshift_hook.run(strScript, autocommit=True)
        
     # read the sql file
    scriptfilename1 = default_args['current_path'] +"/011-2-import-layout-csv.sql"
    filehandle = open(scriptfilename1, 'r')
    strScript1 = filehandle.read()
    filehandle.close() 
    
    crte_stmt1 = []
    crte_stmt1 = ' create table donorb_{order_id}_order ( '
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('axle-donorbase-raw-sources')
    body = ''
    y = 0
    # Iterates through all the objects, Each obj 
    # is an ObjectSummary, so it doesn't contain the body. You'll need to call get to get the whole body.
    for obj in bucket.objects.filter(Prefix=file_key):
        body = obj.get()['Body'].readlines()
        for line in body:
            a = line.decode("utf-8")
            print(a)
            x = a[0:8]
            if x == "Position":
                y = 1
            if y == 1 and a != '\r\n':
                column_list = a[9:25].rstrip()
                column_list_modified = column_list.replace(' ', '_')
                crte_stmt1 = crte_stmt1 + ' ' + '[' + column_list_modified + ']'
                crte_stmt1 = crte_stmt1 + ' ' + 'varchar(' + a[50:53].rstrip() + '),'
            if y == 1 and a == '\r\n':
                y = 0
                crte_stmt1 = crte_stmt1 + ')'
                crte_stmt1 = crte_stmt1.replace('),)', ') )')
                crte_stmt_beg=crte_stmt1.split('(')[0]+'('
                crte_stmt_end=crte_stmt1[67:]
                crte_stmt1 = crte_stmt_beg+crte_stmt_end
                print(crte_stmt1)
                break
        
    if table_record_result.upper() == 'CSV':
        csvfilename =  ''
        if layout_file_name == str(order_id)+".layout"+".txt":
            csvfilename = "{order_id}.order.csv"
        elif layout_file_name == "Layout." +str(order_id)+".txt":
            csvfilename = "Order.{order_id}.CSV.txt"
            
        copy_command1 = []
        copy_command1 = r"""COPY donorb_{order_id}_order FROM ''s3://axle-donorbase-raw-sources/Prior LiftEngine Orders/"""+str(csvfilename)+r"""'' iam_role ''{iam}'' csv delimiter '','' IGNOREHEADER 1 """
        y = 0
        for obj in bucket.objects.filter(Prefix=file_key):
            body = obj.get()['Body'].readlines()
        for line in body:
            a = line.decode("utf-8")
            x = a[0:8]
            if x == "Position":
                y = 1
            if y == 1 and a != '\r\n':
                column_list1 = a[9:25].rstrip()
                column_list_modified1 = column_list1.replace(' ', '_')
                copy_command1 = copy_command1 + ' ' + column_list_modified1 
                copy_command1 = copy_command1[:-11]
                break
        
    # replace variable with values into sql file
        strScript1 = strScript1.replace('{crte_stmt1}', str(crte_stmt1))
        strScript1 = strScript1.replace('{copy_command1}', str(copy_command1))
        strScript1 = strScript1.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
        strScript1 = strScript1.replace('{s3-aopinput}', s3_path)
        # strScript1 = strScript1.replace('{csvfilename}', str(csvfilename))
        strScript1 = strScript1.replace('{order_id}', order_id)

        redshift_hook.run(strScript1, autocommit=True)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# run sql scripts
data_load1 = PythonOperator(
    task_id='load_layout',
    python_callable = update_sql_file,
    dag=dag
)

get_result = PythonOperator(
    task_id="fetched_output_result",
    python_callable=get_output_format,
    provide_context=True,
    dag=dag)

# run sql scripts
data_load2 = PythonOperator(
    task_id='load_orders_fixed_csv',
    python_callable = update_sql_file_output,
    op_kwargs={
        "fetch_result": "fetched_output_result"
    },
     provide_context=True,
    dag=dag
)

# send success email
send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator  >> data_load1 >> get_result >> data_load2 >> send_notification >> end_operator
