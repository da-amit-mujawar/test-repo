import json
import logging
import os
from datetime import date

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from helpers.sqlserver import get_build_info
from operators import EMROperator

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def set_emr_process_date(**kwargs):
    build_info = json.loads(Variable.get(ls_summ_calc_var))
    build_info_dic = {}
    ls_process_dt = kwargs['dag_run'].conf.get("process_date") or date.today().strftime("%Y%m%d")
    for item in build_info:
        if item in 'process_date':
            build_info_dic['process_date'] = ls_process_dt
        else:
            build_info_dic[item] = build_info[item]

    Variable.set(ls_summ_calc_var, json.dumps(build_info_dic, indent=4))


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "databaseid": 82,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
    "email_on_failure": True,
    "email_on_retry": False,
    "process_date": ""
}

dbid = default_args['databaseid']
ls_current_path = default_args['currentpath']
ls_summ_calc_var = "var-summary-calculation-apogee-fec"

dag = DAG('builds-summary-calc-Apogee-FEC-EMRProcess',
          default_args=default_args,
          description='Calculate Summary Information for Apogee FEC Data - EMR Processing',
          schedule_interval='@once',
          max_active_runs=1
          )

begin_prcs = DummyOperator(task_id='Begin_Execution', dag=dag)

set_emr_prcsdate = PythonOperator(task_id='set-process-date',
                                  python_callable=set_emr_process_date,
                                  dag=dag)

fetch_build = PythonOperator(task_id="Fetch-DB-Build-Info",
                             python_callable=get_build_info,
                             op_kwargs={"databaseid": dbid},
                             provide_context=True,
                             dag=dag,
                             )

csv_to_parq = EMROperator(task_id='Convert_CSV_2_Parquet',
                          base_config_path=ls_current_path + '/emrconfig.json',
                          custom_config_path=ls_current_path + '/apogee-fec-step-0.json',
                          spark_config=ls_current_path + '/config.json',
                          process_info=Variable.get(ls_summ_calc_var, deserialize_json=True),
                          dag=dag)

prepare_data = EMROperator(task_id='Prepare_Input_Data',
                           base_config_path=ls_current_path + '/emrconfig.json',
                           custom_config_path=ls_current_path + '/apogee-fec-step-1.json',
                           spark_config=ls_current_path + '/config.json',
                           process_info=Variable.get(ls_summ_calc_var, deserialize_json=True),
                           dag=dag)

list_process = EMROperator(task_id='Process_Calc',
                           base_config_path=ls_current_path + '/emrconfig.json',
                           custom_config_path=ls_current_path + '/apogee-fec-step-2.json',
                           spark_config=ls_current_path + '/config.json',
                           process_info=Variable.get(ls_summ_calc_var, deserialize_json=True),
                           dag=dag)
# custom_process = EMROperator(task_id='Custom_Process_Calculation',
#                              base_config_path=ls_current_path + '/emrconfig.json',
#                              custom_config_path=ls_current_path + '/apogee-fec-step-3.json',
#                              spark_config=ls_current_path + '/config.json',
#                              process_info=Variable.get(ls_summ_calc_var, deserialize_json=True),
#                              dag=dag)
end_prcs = DummyOperator(task_id='Stop_Execution', dag=dag)

begin_prcs >> set_emr_prcsdate >> fetch_build >> csv_to_parq >> prepare_data >> list_process  >> end_prcs

# begin_prcs >> set_emr_prcsdate >> fetch_build >> csv_to_parq >> prepare_data >> list_process >> custom_process >> end_prcs
