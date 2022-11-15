import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from operators import EMROperator

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0
}

dag = DAG('spin_emr_cluster',
          default_args=default_args,
          description='Start the EMR cluster',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

csv_to_parquet = EMROperator(
    task_id='Cluster_to_initiate_csv_to_par_process',
    base_config_path=default_args['currentpath'] + '/emrconfig.json',
    custom_config_path=default_args['currentpath'] + '/apogee-step-0.json',
    dag=dag)

prepare_input_data = EMROperator(
    task_id='Cluster_to_prepare_input_data',
    base_config_path=default_args['currentpath'] + '/emrconfig.json',
    custom_config_path=default_args['currentpath'] + '/apogee-step-1.json',
    dag=dag)

list_cal = EMROperator(
    task_id='Cluster_to_calculate_list_category_and_month_data_calculation',
    base_config_path=default_args['currentpath'] + '/emrconfig.json',
    custom_config_path=default_args['currentpath'] + '/apogee-step-2.json',
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> csv_to_parquet >> prepare_input_data >> list_cal >> end_operator
