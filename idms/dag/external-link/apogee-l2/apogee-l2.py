import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.apogee_l2 import *

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid':77,
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('external-link-apogee-L2',
    default_args=default_args,
    description='Load apogee l2 data to redshift',
    schedule_interval='@once',
    max_active_runs=1
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
''' remove list processing
combine_all_l2_tables_sql = PythonOperator(
    task_id='combine_all_l2_tables_sql',
    python_callable=combine_all_l2_tables_sql,
    dag=dag,
)

generating_individual_id_and_company_id_for_empty_ids_sql = PythonOperator(
    task_id='generating_individual_id_and_company_id_for_empty_ids_sql',
    python_callable=generating_individual_id_and_company_id_for_empty_ids_sql,
    dag=dag,
)

load_field_names_to_temp_table_for_export_sql = PythonOperator(
    task_id='load_field_names_to_temp_table_for_export_sql',
    python_callable=load_field_names_to_temp_table_for_export_sql,
    dag=dag,
)

generate_script_for_apogee_l2_voter_table_expoert_sql = PythonOperator(
    task_id='generate_script_for_apogee_l2_voter_table_expoert_sql',
    python_callable=generate_script_for_apogee_l2_voter_table_expoert_sql,
    dag=dag,
)
'''
#run sql scripts
data_load1 = RedshiftOperator(
    task_id='load_tblexternal45',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
'''
data_load2 = RedshiftOperator(
    task_id='load_voterfrequency',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/1*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load3 = RedshiftOperator(
    task_id='processing_list_update',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/2*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
'''
# check counts
data_check = DataCheckOperator(
    task_id='check_maintable_record_count',
    dag=dag,
    config_file_path = default_args['currentpath'] + '/config.json',
    min_expected_count = [
                            ('tablename1', 1000000)
                        ],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

activate = RedshiftOperator(
    task_id='rename_table',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/3*.sql',
    config_file_path = default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

(
    start_operator 
    >> data_load1
    >> data_check
    >> activate 
    >> send_notification 
    >> end_operator
)

