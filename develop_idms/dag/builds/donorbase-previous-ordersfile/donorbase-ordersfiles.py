import os
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
from time import *


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

dag = DAG('donorbase-generate-final-opu',
          default_args=default_args,
          description='donorbase-generate-previous-ordersfile',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

#Checking for redshift connection
redshift_conn_id = Variable.get('var-redshift-postgres-conn')
redshift_hook = PostgresHook(postgres_conn_id = redshift_conn_id)
iam = Variable.get('var-password-redshift-iam-role')
s3_path = Variable.get("var-s3-bucket-names", deserialize_json=True).get("s3-donorbase-silver")

def update_sql_file(**kwargs):
    #read the sql file
    scriptfilename = default_args['current_path'] + "/010-export-orderid.sql"
    filehandle = open(scriptfilename, 'r')
    strScript = filehandle.read()
    filehandle.close()

    # read argument value
    if kwargs['dag_run'].conf.get('bucket_name') == None or kwargs['dag_run'].conf.get('file_key') == None:
        raise ValueError('Please pass bucket_name,file_key argument')
    else:
        bucket_name = kwargs['dag_run'].conf.get('bucket_name')
        file_key = kwargs['dag_run'].conf.get('file_key')

    #replace variable with values into sql file 
    strScript = strScript.replace('{bucket_name}', bucket_name)
    strScript = strScript.replace('{file_key}', file_key)
    strScript = strScript.replace('{iam}', iam)
    
    redshift_hook.run(strScript, autocommit=True)

def get_list_orderid():
    sql = f"""SELECT Distinct order_id FROM temp_donorbase"""
    return redshift_hook.get_pandas_df(sql)

def data_unload(**kwargs):
    df_order_id = get_list_orderid() 
    listorderid = df_order_id['order_id'].to_list() #[1,2,3,4,5]
    for order_id in listorderid:
        sql1 = 'SELECT IDMS_iMatchCode FROM temp_donorbase A WHERE order_id =' + str(order_id) + ' GROUP BY IDMS_iMatchCode'
        sql2 = 'SELECT IDMS_cMatchCode FROM temp_donorbase A WHERE order_id = ' + str(order_id) + ' GROUP BY IDMS_cMatchCode'
        sql_unload_ind = f"""UNLOAD ('{sql1}') 
                                    TO 's3://{s3_path}/suppression/opu{order_id}_ind.txt'
                                    IAM_ROLE '{iam}'
                                    CSV DELIMITER AS ','
                                    ALLOWOVERWRITE
                                    PARALLEL OFF"""
                                    
        sql_unload_hh = f"""UNLOAD ('{sql2}') 
                                    TO 's3://{s3_path}/suppression/opu{order_id}_hh.txt'
                                    IAM_ROLE '{iam}'
                                    CSV DELIMITER AS ','
                                    ALLOWOVERWRITE
                                    PARALLEL OFF"""
                      
        redshift_hook.run(sql_unload_ind, autocommit=True)
        copy_file(s3_path, f"""{s3_path}/suppression/opu{order_id}_ind.txt000""", f"""suppression/opu{order_id}_ind.txt""")
        delete_file(s3_path, f"""suppression/opu{order_id}_ind.txt000""")


        redshift_hook.run(sql_unload_hh, autocommit=True)
        copy_file(s3_path, f"""{s3_path}/suppression/opu{order_id}_hh.txt000""", f"""suppression/opu{order_id}_hh.txt""")
        delete_file(s3_path, f"""suppression/opu{order_id}_hh.txt000""")
    
    bucket_name = kwargs['dag_run'].conf.get('bucket_name')
    file_key = kwargs['dag_run'].conf.get('file_key')

    move_file(bucket_name,file_key,bucket_name,"processed/")


# run sql scripts
export_data = PythonOperator(
    task_id='load_data',
    python_callable = update_sql_file,
    dag=dag,
)

data_unload  = PythonOperator(
    task_id='data_unload',
    python_callable = data_unload,
    dag=dag,
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

start_operator  >> export_data >> data_unload >> send_notification >> end_operator
