import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.sqlserver import *
from helpers.redshift import *
from reports.list_updates_mgen_audit_report.tasks import *
from datetime import date,datetime

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'dbid': 847,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('builds-mgen',
          default_args=default_args,
          description='',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

def replace_today_config(config_file):
    folder_name=datetime.strftime(date.today(),"%Y%m%d")

    with open(config_file) as f:
        config = json.load(f)
    if config["s3_folder_today"] == '{today}': 
        with open(config_file) as file:
            s= file.read()
        s = s.replace('{today}',folder_name)
        with open (config_file,"w") as file:
            file.write(s)
    else:
        config["s3_folder_today"] = '{today}'
        with open (config_file,"w") as file:
            json.dump(config,file)

fetch_buildinfo = PythonOperator(
  task_id="fetch_build",
  python_callable=get_build_info,
  op_kwargs={'databaseid': default_args['dbid']
             },
  provide_context=True,
  dag=dag)

replace_today1 = PythonOperator(
    task_id='replace_today1',
    python_callable=replace_today_config,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json'
               },
    provide_context=True,
    dag=dag
)

data_load1 = RedshiftOperator(
    task_id='Create-And-Load-Child1',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/0001-child1.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load2 = RedshiftOperator(
    task_id='Create-And-Load-Child2',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/0002-child2.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load3 = RedshiftOperator(
    task_id='Create-And-Load-Child3',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/0003-child3.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)


data_load4 = RedshiftOperator(
    task_id='Create-And-Load-Child35',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/0004-child35.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load5 = RedshiftOperator(
    task_id='Create-And-Load-Child6',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/0005-child6.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)


data_load6 = RedshiftOperator(
    task_id='Create-And-Load-Child5',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/0006-child5.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load7 = RedshiftOperator(
    task_id='Create-And-Load-Child34',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/0009-*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load8 = RedshiftOperator(
    task_id='Create-and-load-00010',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00010-mgen_new_load.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load9 = RedshiftOperator(
    task_id='Create-and-load-00011',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00011-*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load10 = RedshiftOperator(
    task_id='Create-and-load-00012',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00012-*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load11 = RedshiftOperator(
    task_id='Create-and-load-00013',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00013-*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load12 = RedshiftOperator(
    task_id='Create-and-load-00014',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00014-*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load13 = RedshiftOperator(
    task_id='Create-and-load-00015',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00015-mgen_credit_card_flag.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load14 = RedshiftOperator(
    task_id='Create-and-load-00016',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00016-mgen_donor_flags.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load15 = RedshiftOperator(
    task_id='Create-and-load-00017',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00017-mgen_market_target_age_flags.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load16 = RedshiftOperator(
    task_id='Create-and-load-00018',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00018-mgen_children_month_of_birth.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load17 = RedshiftOperator(
    task_id='Create-and-load-00019',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00019-mgen_children_age_by_gender.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load18 = RedshiftOperator(
    task_id='Create-and-load-00020',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00020-mgen_experian_donor_array.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load19 = RedshiftOperator(
    task_id='Create-and-load-00021',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00021-mgen_experian_child_array.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load20 = RedshiftOperator(
    task_id='Create-and-load-00022',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00022-mgen_experian_lifestyle_array.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load21 = RedshiftOperator(
    task_id='Create-and-load-00023',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00023-mgen_arr_twm.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load22 = RedshiftOperator(
    task_id='Create-and-load-00024',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00024-mgen_dlx_segments.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load23 = RedshiftOperator(
    task_id='Create-and-load-00025',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00025-mgen_arr_pay.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load24 = RedshiftOperator(
    task_id='Create-and-load-00026',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00026-mgen_arr_cc.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load25 = RedshiftOperator(
    task_id='Create-and-load-00027',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00027-child31.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load26 = RedshiftOperator(
    task_id='Create-and-load-00028',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00028-child32.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load27 = RedshiftOperator(
    task_id='Create-and-load-00029',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00029-child33.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load28 = RedshiftOperator(

    task_id='create-and-load-mgen_pub2',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00033-mgen_pub2.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

parallel_task1 = [data_load1,data_load2,data_load3,data_load4,data_load5,data_load6,data_load7,data_load8,data_load9,data_load10,data_load11,data_load12,data_load13,data_load14,data_load15,data_load16,data_load17,data_load18,data_load19,data_load20,data_load21,data_load22,data_load23,data_load24,data_load25,data_load26,data_load27,data_load28]

data_load29 = RedshiftOperator(
    task_id='Create-and-load-all-00030-files',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00030-*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load30 = RedshiftOperator(
    task_id='Unloading-sample-files-36-1',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-1-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load31 = RedshiftOperator(
    task_id='Unloading-sample-files-36-2',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-2-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load32 = RedshiftOperator(
    task_id='Unloading-sample-files-36-3',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-3-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load33 = RedshiftOperator(
    task_id='Unloading-sample-files-36-4',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-4-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load34 = RedshiftOperator(
    task_id='Unloading-sample-files-36-5',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-5-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load35 = RedshiftOperator(
    task_id='Unloading-sample-files-36-6',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-6-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load36 = RedshiftOperator(
    task_id='Unloading-sample-files-36-7',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-7-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)
data_load37 = RedshiftOperator(
    task_id='Unloading-sample-files-36-8',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00036-8-unloading-sample-file.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load38 = RedshiftOperator(
    task_id='create-manual-model-tables',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00037-manual-model-scoring.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

data_load39 = RedshiftOperator(
    task_id='mgen-report',
    dag=dag,
    databaseid=default_args['dbid'],
    sql_file_path=default_args['currentpath'] + '/00039-mgen-report.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

parallel_task2 = [data_load30,data_load31,data_load32,data_load33,data_load34,data_load35,data_load36,data_load37,data_load38,data_load39]

# check counts
data_check = DataCheckOperator(
    task_id='check_maintable_record_count',
    dag=dag,
    config_file_path=default_args['currentpath'] + '/config.json',
    min_expected_count=[
        # ('tblmain_{build_id}_{build}', 250000000)
    ],
    redshift_conn_id=Variable.get('var-redshift-postgres-conn')
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={'tablename': Variable.get('var-db-847', deserialize_json=True)['maintable_name']},
    provide_context=True,
    dag=dag)

build_activate = PythonOperator(
    task_id="activate_build",
    python_callable=activate_build,
    op_kwargs={'database_id': default_args["dbid"],
               'build_status': 70,
               'min_expected_count': 250000000,
               'count_task_id': 'count_maintable',
               'sql_conn_id': Variable.get('var-sqlserver-conn')
               },
    provide_context=True,
    dag=dag)

send_notification = PythonOperator(
   task_id='email-notification',
   python_callable=send_email,
   op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
              'dag': dag
              },
    dag=dag)

generate_audit_report = PythonOperator(
    task_id='generate_audit_report',
    python_callable=generateAuditReport,
    op_kwargs={'buildId': Variable.get('var-db-847', deserialize_json=True)['build_id'],
               'dag': dag},
    dag=dag,
)

send_notification_audit_report = PythonOperator(
    task_id='send_notification_audit_report',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/mGenAuditConfig.json',
               'report_list': [
                   f"/tmp/Mgen_AuditReport_847_{Variable.get('var-db-847', deserialize_json=True)['build_id']}.xlsx"],
               'dag': dag
               },
    dag=dag)

replace_today2 = PythonOperator(
    task_id='replace_today2',
    python_callable=replace_today_config,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json'
               },
    provide_context=True,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)
(
start_operator 
>> replace_today1
>> fetch_buildinfo  
>> parallel_task1
>> data_load29 
>> parallel_task2 
>> data_check 
>> get_count 
>> build_activate
>> send_notification
>> replace_today2
>> generate_audit_report  
>> send_notification_audit_report  
>> end_operator
)