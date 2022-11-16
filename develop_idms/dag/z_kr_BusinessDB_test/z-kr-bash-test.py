from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import os
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 9),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False
}

dag = DAG('z-kr-bash-runner',
          default_args=default_args,
          description='Test DAG for bash operator',
          schedule_interval='@once',
          max_active_runs=1
          )

cmd = Variable.get('bash_cmd')

bash_step = BashOperator(
    task_id='bash_task',
    dag=dag,
    bash_command=cmd
)


bash_step