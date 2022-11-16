from datetime import datetime, timedelta
import os
import csv
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators import S3CopyObjectOperator



def get_redshift_hook(**kwargs):
    db_conn = JdbcHook(jdbc_conn_id=Variable.get('var-redshift-jdbc-conn'))
    #db_conn = PostgresHook(postgres_conn_id=Variable.get('var-redshift-postgres-conn'))
    return db_conn


def process_sql():
    # id = redshift_conn.get_records(sql="SELECT * FROM apogee_input_count")
    redshift_conn= get_redshift_hook()
    sql = """
        drop table if exists test_jdbc_con;
        create table test_jdbc_con (id int, name varchar(50));
        insert into test_jdbc_con(id, name) values(1, 'susan');
        select * from test_jdbc_con;
    """
    redshift_conn.run(sql)
    return_records = redshift_conn.get_records('select * from test_jdbc_con')
    print (f'Name is : {return_records[0][1]}')

def get_s3_hook():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    print('after hook')
    s3bucket = s3_hook.get_bucket('develop_idms-2722-internalfiles')
    for obj in s3bucket.objects.filter(Prefix='Reports/sf_test/'):
        print('in loop')
        print(obj.key)
        if obj.key[-1:] != '/':
            key = obj.key.split('/')[-1]
            source_file = 's3://develop_idms-2722-internalfiles/Reports/sf_test/' + key
            dest_file = 's3://develop_idms-2722-internalfiles/Reports/sf_test_copy/' + key
            print(source_file)
            print(dest_file)
            # # s3.Object(params['bucket'], dest_file).copy_from(CopySource=source_file)
            s3_hook.copy_object( source_file,
                dest_file
                # self.source_bucket_name,
                # self.dest_bucket_name,
                # self.source_version_id,
                # self.acl_policy,
            )
    #return s3_hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),

}

dag = DAG(
    'z-susan-jdbc-conn-test',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@once',
    max_active_runs=1
)

# t1 = PythonOperator(
#     task_id='get_s3_hook',
#     python_callable=get_s3_hook,
#     dag=dag,
# )

s3_copy = S3CopyObjectOperator(
    task_id='copy_files_in_s3',
    dag=dag,
    source_bucket_key='Reports/sf_test/',
    dest_bucket_key='Reports/sf_test_copy/',
    source_bucket_name='develop_idms-2722-internalfiles',
    dest_bucket_name='develop_idms-2722-internalfiles',
    #dest_file_extension='csv',
    bulk_copy = True
)

#develop_idms-2722-internalfiles Reports/sf_test/

# t1 = PythonOperator(
#     task_id='get_connection',
#     python_callable=get_connection,
#     dag=dag,
# )

# t2 = PythonOperator(
#     task_id='process-sql-statement',
#     python_callable=process_sql,
#     dag=dag,
# )

# t3 = PythonOperator(
#     task_id='DisplayRecords',
#     python_callable=displyrecords,
#     dag=dag,
# )

s3_copy
