from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import os
import json
import boto3
from datetime import datetime
import calendar
from operators import RedshiftOperator, GenericRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 26),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'schedule_interval': '0 0 21 1/1 * ? *',
    'email_on_retry': False
}

dag = DAG('z-kr-core-businessdb-dag',
          default_args=default_args,
          description='Sample DAG for Business DB',
          schedule_interval='@once',
          max_active_runs=1
          )

config_file_path=default_args['currentpath'] + '/config.json'
with open(config_file_path) as f:
    config = json.load(f)

def fetchToday():
    today_date = datetime.today()
    print(today_date)
    today_formated = today_date.strftime('%Y%m%d')
    print(today_formated)
    print(datetime.today().weekday())
    day = calendar.day_name[datetime.today().weekday()]
    print(calendar.day_name[datetime.today().weekday()] )
    return today_formated, day

def fetchToday():
    today_date = datetime.today()
    #print(today_date)
    today_formated = today_date.strftime('%Y%m%d')
    #print(today_formated)
    #print(datetime.today().weekday())
    day = calendar.day_name[datetime.today().weekday()]
    #print(calendar.day_name[datetime.today().weekday()] )
    return today_formated, day

def generateList(bucket, prefix, suffix, date, output_bucket, output_path):
    input_list = []
    for file in bucket.objects.filter(Prefix=prefix+str(date)):
        if file.key.endswith(suffix):
            print(file.key)
            input_list.append("s3://"+bucket.name+"/"+file.key)
    generateJSON(output_bucket, output_path, input_list)

def generateJSON(output_bucket, output_path, input_list):
    final_dict = {"entries": []}
    finallist = []
    for l in input_list:
        tempdict = {}
        tempdict['url']=l
        tempdict['mandatory']=True
        finallist.append(tempdict)
    final_dict["entries"]=finallist

    s3 = boto3.resource('s3')
    s3object = s3.Object(output_bucket, output_path)

    s3object.put(
        Body=(bytes(json.dumps(final_dict, indent = 4).encode('UTF-8')))
    )

def generateManifestFile():
    s3 = boto3.resource('s3')
    input_bucket = s3.Bucket(config.get('input_bucket'))
    input_prefix_full = config.get('input_prefix_full')
    input_prefix_changes = config.get('input_prefix_changes')
    file_types = config.get('file_types')
    file_types_list = file_types.split(",")
    mode = "create_full_table"
    input_prefix = "input_prefix_full"
    date,day = fetchToday()
    if day == config.get("full_load_day"):
        input_prefix = input_prefix_full
        mode = "create_full_table"
    else:
        input_prefix = input_prefix_changes
        mode = "create_changes_table"
    for file_type in file_types_list:
        input_suffix = file_type+config.get('file_extension')
        output_bucket = config.get('output_bucket')
        output_path = config.get('output_path')+file_type+'_manifest.json'
        generateList(input_bucket, input_prefix, input_suffix, date, output_bucket, output_path)
    return mode


def generateSetters(file_type):
    ddl_file=default_args['currentpath'] + '/DDL/bf_full_'+file_type+'_ddl.sql'
    fd2 = open(ddl_file, 'r')
    lines = fd2.readlines()
    temp = ""
    lst = []
    for line in lines:
        if temp == "y" and line.strip() != ")":
           l = line.split()
           lst.append(l[0])
        if line.strip() == "(":
            temp = "y"
        if line.strip() == ")":
            temp = ""
    print(lst)

    setters = ""
    for l in lst:
        setters = setters + l+"="+"c."+l+","

    setters = setters[:-1]
    fd2.close()
    print(setters)
    return setters, lst


def upsert_function(**kwargs):
    print(kwargs['redshift_conn_id'])
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_changes_upsert.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    file_types = config['file_types']
    file_types_list = file_types.split(",")
    for file_type in file_types_list:
        #Logic to generate setter columns string
        setter_fields, column_list = generateSetters(file_type)
        columns = ','.join(column_list)
        sql2 = ""
        sql2 = sql.replace('{table}', file_type)
        sql2 = sql2.replace('{set_columns}', setter_fields)
        sql2 = sql2.replace('{column_list}', columns)
        redshift_hook.run(sql2)

def derive_counts(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_counts.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    file_types = config['file_types']
    file_types_list = file_types.split(",")
    for file_type in file_types_list:
        sql2 = ""
        sql2 = sql.replace('{counts_table}', config.get('counts_table'))
        sql2 = sql2.replace('{file_type}', file_type)
        redshift_hook.run(sql2)

def prepare_counts_table(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_counts_table_ddl.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    sql = sql.replace('{counts_table}', config.get('counts_table'))
    redshift_hook.run(sql)


generate_op = BranchPythonOperator(
    task_id='generate_load',
    python_callable=generateManifestFile,
    provide_context=True,
    dag=dag,
)

create_full_table = RedshiftOperator(
    task_id='create_full_table',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/DDL/bf_full_*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

create_changes_table = RedshiftOperator(
    task_id='create_changes_table',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/DDL/bf_changes_*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

full_copy = GenericRedshiftOperator(
    task_id='full_copy',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/SQL/bf_full_copy.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

changes_copy = GenericRedshiftOperator(
    task_id='changes_copy',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/SQL/bf_changes_copy.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

upsert_task = PythonOperator(
    task_id='upsert_task',
    python_callable=upsert_function,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)


full_postcopy = GenericRedshiftOperator(
    task_id='full_postcopy',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/SQL/bf_full_postcopy.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

changes_postcopy = GenericRedshiftOperator(
    task_id='changes_postcopy',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/SQL/bf_changes_postcopy.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

full_counts_prepare = PythonOperator(
    task_id='full_counts_prepare',
    python_callable=prepare_counts_table,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

full_counts = PythonOperator(
    task_id='full_counts',
    python_callable=derive_counts,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

changes_counts_prepare = PythonOperator(
    task_id='changes_counts_prepare',
    python_callable=prepare_counts_table,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

changes_counts = PythonOperator(
    task_id='changes_counts',
    python_callable=derive_counts,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

generate_op >> [create_full_table,create_changes_table]
create_full_table.set_downstream(full_copy)
create_changes_table.set_downstream(changes_copy)
full_copy.set_downstream(full_postcopy)
full_postcopy.set_downstream(full_counts_prepare)
full_counts_prepare.set_downstream(full_counts)
changes_copy.set_downstream(upsert_task)
upsert_task.set_downstream(changes_postcopy)
changes_postcopy.set_downstream(changes_counts_prepare)
changes_counts_prepare.set_downstream(changes_counts)