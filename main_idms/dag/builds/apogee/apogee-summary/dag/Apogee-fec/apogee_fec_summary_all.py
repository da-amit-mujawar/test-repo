import glob
import json
import logging
import os
from datetime import date

import airflow
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from helpers.sqlserver import get_build_info
from operators import EMROperator
from operators import RedshiftOperator

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


def set_db_process_date(**kwargs):
    build_info = json.loads(Variable.get(ls_summ_calc_var))
    build_info_dic = {}
    ls_process_dt = kwargs['dag_run'].conf.get("process_date") or \
                    build_info['process_date'] or \
                    date.today().strftime("%Y%m%d")
    for item in build_info:
        if item in 'process_date':
            build_info_dic['process_date'] = ls_process_dt
        else:
            build_info_dic[item] = build_info[item]
    Variable.set(ls_summ_calc_var, json.dumps(build_info_dic, indent=4))


def download_sql_files(process_date, dct_process, tgt_location, **kwargs):
    s3c = boto3.client('s3')
    s3 = boto3.resource('s3')

    logger.info("Rename all old SQL files starting with 00 from {0}".format(tgt_location))
    for sql_file in sorted(glob.glob(tgt_location + "00*.sql")):
        logger.info("Renaming SQL File {0} to {0}.old.bak".format(sql_file))
        os.replace(sql_file, sql_file + ".old.bak")

    logger.info("Date:{}".format(process_date))

    # Setup Bucket, ProcessDate and S3 Prefix
    ls_bucket = dct_process['bucket_name']
    set_db_process_date(**kwargs)

    ls_process_date = Variable.get(ls_summ_calc_var, deserialize_json=True)['process_date']

    key_prefix = "{0}/output/sql/".format(dct_process['s3_sql_location'] + ls_process_date)
    logger.info("Downloading SQL Files from location {0} - {1}".format(ls_bucket, key_prefix))

    lf_path = tgt_location
    file_no = 0
    for file in s3.Bucket(ls_bucket).objects.filter(Prefix=key_prefix):
        file_no = file_no + 1
        ls_file_no = str(file_no).rjust(3, '0')
        s3_file = file.key.split('/')[-1]
        logger.info("Processing File {0}".format(s3_file))

        if s3_file == "":
            continue
        else:
            ls_key = key_prefix + s3_file
            logger.info("Bucket:{0}, Key:{1}".format(ls_bucket, ls_key))
            obj = s3c.get_object(Bucket=ls_bucket, Key=ls_key)
            sql_content = str(obj['Body'].read().decode('utf-8'))
            lf_content = sql_content \
                .replace('{iam}', Variable.get('var-password-redshift-iam-role'))

            # File Location - Local
            lf_sql_file = s3_file
            la_content = lf_content.split(';')
            ln_stmt = 0
            for content in la_content:
                ln_stmt = ln_stmt + 1
                ls_stmt = str(ln_stmt).rjust(3, '0')
                lcl_file = "{0}/{1}_{2}_{3}_{4}".format(lf_path, ls_file_no, ls_stmt,
                                                        ls_process_date, lf_sql_file)
                print("Generating SQL File {0}".format(lcl_file))
                ls_sql = content.lstrip().rstrip()
                if len(ls_sql) > 0:
                    with open(lcl_file, "w") as lf_output:
                        lf_output.write(ls_sql + ";")


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

lj_prcs_info = json.loads(Variable.get(ls_summ_calc_var))
ls_curr_date = lj_prcs_info['process_date']
lcl_sql_location = default_args['currentpath'] + "/sql/"

dag = DAG('builds-summary-calc-Apogee-FEC-All',
          default_args=default_args,
          description='Calculate Summary Information for Apogee FEC Data - EMR and DBProcessing',
          schedule_interval='@once',
          max_active_runs=1
          )

begin_prcs = DummyOperator(task_id='Begin_Execution', dag=dag)

set_emr_prcsdate = PythonOperator(task_id='set-process-date',
                                  python_callable=set_emr_process_date,
                                  dag=dag)

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
#custom_process = EMROperator(task_id='Custom_Process_Calculation',
#                             base_config_path=ls_current_path + '/emrconfig.json',
#                             custom_config_path=ls_current_path + '/apogee-step-3.json',
#                             spark_config=ls_current_path + '/config.json',
#                             process_info=Variable.get(ls_summ_calc_var, deserialize_json=True),
#                             dag=dag)

dwnld_sql = PythonOperator(task_id='Download_SQL_Files_from_s3',
                           python_callable=download_sql_files,
                           op_kwargs={'process_date': ls_curr_date,
                                      'dct_process': lj_prcs_info,
                                      'tgt_location': lcl_sql_location},
                           dag=dag)

data_load = RedshiftOperator(task_id='Redshift_Load_ProcessData',
                             dag=dag,
                             sql_file_path=lcl_sql_location + "00*.sql",
                             config_file_path=default_args['currentpath'] + "/config.json",
                             databaseid=dbid,
                             redshift_conn_id=Variable.get('var-redshift-postgres-conn'))

addl_load = RedshiftOperator(task_id='Redshift_Load_CustomData',
                             dag=dag,
                             sql_file_path=lcl_sql_location + "10*.sql",
                             config_file_path=default_args['currentpath'] + "/config.json",
                             databaseid=dbid,
                             redshift_conn_id=Variable.get('var-redshift-postgres-conn'))

end_prcs = DummyOperator(task_id='Stop_Execution', dag=dag)

begin_prcs >> set_emr_prcsdate >> csv_to_parq >> prepare_data >> list_process >> dwnld_sql >> data_load >> addl_load >> end_prcs
