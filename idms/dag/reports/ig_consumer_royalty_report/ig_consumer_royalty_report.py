import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.ig_consumer_royalty_report.tasks import *
from datetime import datetime, timedelta


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(8),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid':1267,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False,
    'mode': 'W'
}

# to trigger dag w/dates:
# {"run-info": {"mode": "DR", "start-date": "2022.02.27", "end-date": "2022.03.05"} }
mode = default_args.get('mode', 'D')
database_id = default_args.get('databaseid', 1267)
sql_output_folder = f'internaltransfer/Temp/RoyaltyOrderIds_{database_id}.txt'

if mode == 'W':
    time_stamp = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    report_name = f'RoyaltyOutput_Weekly_{time_stamp}.csv'
elif mode == 'D':
    time_stamp = datetime.today().strftime('%Y%m%d')
    report_name = f'RoyaltyOutput_Daily_{time_stamp}.csv'
else:
    time_stamp = datetime.today().strftime('%Y%m')
    report_name = f'RoyaltyOutput_Monthly_{time_stamp}.csv'

report_path = f'tmp/{report_name}'


dag = DAG('reports-ig-consumer-royalty-report-do-not-run-manually',
          default_args=default_args,
          description='IG CONSUMER ROYALTY REPORTS - DO NOT RUN MANUALLY',
          schedule_interval='0 23 * * 0',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


get_royalty_orderids_from_sq = PythonOperator(
    task_id='get_royalty_orderids_from_sq',
    python_callable=get_royalty_orderids_from_sq,
    op_kwargs={'mode': mode,
               'database_id': database_id,
               'sql_output_folder': sql_output_folder},
    dag=dag,
)

# run sql scripts
import_in_redshift = RedshiftOperator(
    task_id='import_in_redshift',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/0*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_main_table_list_redshift = PythonOperator(
    task_id='get_main_table_list_redshift',
    python_callable=get_main_table_list_redshift,      
    dag=dag,
)

get_royalty_records_redshift = PythonOperator(
    task_id='get_royalty_records_redshift',
    python_callable=get_royalty_records_redshift, 
    op_kwargs={'database_id': database_id,
                'report_name': report_name,
                'report_path': report_path},  
    dag=dag,
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
                'report_list': [report_path],
               'dag': dag
               },
    dag=dag)

clean_up_sql = PythonOperator(
    task_id='clean_up_sql',
    python_callable=clean_up_sql,
    op_kwargs={'database_id': database_id}, 
    dag=dag,
)

clean_up_redshift = RedshiftOperator(
    task_id='clean_up_redshift',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/2*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> get_royalty_orderids_from_sq >> import_in_redshift >> get_main_table_list_redshift >> get_royalty_records_redshift >> clean_up_sql >> clean_up_redshift >> send_notification >> end_operator
