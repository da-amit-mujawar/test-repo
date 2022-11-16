import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from reports.royalty_report.tasks import *
from datetime import datetime, timedelta
import json
import logging
from datetime import date


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid':992,
    'email': Variable.get('var-email-on-failure'),
    's3_bucket' : Variable.get('var-manifest-bucket'),
    'iam' : Variable.get('var-redshift-sts-role'),
    'email_on_failure': True,
    'email_on_retry': False,
    'mode': 'W'
}


mode = default_args.get('mode', 'D')
#Run weekly job only on Sunday
# if date.today().weekday() == 6:
#     mode = 'W'

database_id = default_args.get('databaseid', 992)
start_date = ''
end_date = ''
db_list = ''

if mode == 'W':
    time_stamp = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    report_name = f'RoyaltyOutput_Weekly_{time_stamp}.csv'
elif mode == 'D':
    time_stamp = datetime.today().strftime('%Y%m%d')
    report_name = f'RoyaltyOutput_Daily_{time_stamp}.csv'
else:
    time_stamp = datetime.today().strftime('%Y%m')
    report_name = f'RoyaltyOutput_Monthly_{time_stamp}.csv'


config_file = default_args['currentpath'] + '/config.json'
with open(config_file) as f:
    config = json.load(f)

dag = DAG('reports-royalty-report-do-not-run-manually',
          default_args=default_args,
          description='ROYALTY REPORTS - DO NOT RUN MANUALLY',
          schedule_interval=None,
          max_active_runs=1
          )

def get_royalty_reports(mode,
                        start_date,
                        end_date,
                        report_name,
                        ti,
                        **kwargs):

    report_info_dic = {}

    if len(kwargs['dag_run'].conf) >= 1:
        mode = kwargs['dag_run'].conf.get('run-info')['mode']
        start_date = kwargs['dag_run'].conf.get('run-info')['start-date']
        end_date = kwargs['dag_run'].conf.get('run-info')['end-date']
        db_list = kwargs['dag_run'].conf.get('run-info')['db-list']

        report_info_dic["mode"] = mode
        report_info_dic["start-date"] = start_date
        report_info_dic["end-date"] = end_date
        report_info_dic["db-list"] = db_list

        Variable.set('var-reports-royalty', json.dumps(report_info_dic, indent=4))
        # if kwargs['dag_run'].conf.get('db-list') is not None:
        #     royalty_database_list = kwargs['dag_run'].conf.get('db-list')
        # report_date = end_date.replace('.', '')
        # report_name = f'RoyaltyOutput_Weekly_{report_date}.csv'
    else:
        report_info_dic["mode"] = mode
        report_info_dic["start-date"] = start_date
        report_info_dic["end-date"] = end_date
        report_info_dic["db-list"] = config['database_id']

        Variable.set('var-reports-royalty', json.dumps(report_info_dic, indent=4))

    conf = Variable.get('var-reports-royalty',
                deserialize_json=True,
                default_var=None,)

    royalty_database_list = str(conf['db-list']).split(',')

    logging.info('Job running mode is : ', mode)
    logging.info('Start Date is : ', start_date)
    logging.info('End Date is : ', end_date)
    logging.info('royalty_database_list is : ', royalty_database_list)

    for db_id in royalty_database_list:
        logging.info('Royalty Reports processing started for database {} '.format(db_id))

        db_report_name = '{}_{}'.format(db_id, report_name)
        report_path = 'tmp/{}'.format(db_report_name)
        db_id = int(db_id)

        logging.info('Report name is : ', db_report_name)
        logging.info('Report path is : ', report_path)

        #Fetch Data From SQL Server
        sq_fetch_query = config['{}_fetch'.format(db_id)]
        sq_fetch_query = sq_fetch_query.format(db_id=db_id, mode=mode, start_date=start_date, end_date=end_date)
        sql_out = f'internaltransfer/Temp/RoyaltyOrderIds_{mode}_{db_id}.txt'

        get_royalty_orderids_from_sq(db_id,
                                     mode,
                                     start_date,
                                     end_date,
                                     sql_out,
                                     sq_fetch_query)
        logging.info("sql fetch completed")

        #Join sql data with Count and tbl main tables in redshift to populate extra columns
        s3_path = 's3://{}/{}'.format(default_args['s3_bucket'], sql_out)
        rs_sql = import_in_redshift(db_id, s3_path, default_args['iam'])

        query_redshift('var-redshift-postgres-conn', rs_sql)
        get_main_table_list_redshift(db_id)
        get_royalty_records_redshift(db_id, db_report_name, report_path)

        #Clean up temp tables created
        clean_up_sql(db_id)
        clean_up_redshift(db_id)

        #Send royalty reports as an attachment in email
        #send_email(config_file,dag, [report_path])

        logging.info('Royalty Reports completed for database {} '.format(db_id))


def get_path():
    path_list = []
    conf = Variable.get('var-reports-royalty',
                        deserialize_json=True,
                        default_var=None, )

    for id in str(conf['db-list']).split(','):
        report_path = 'tmp/{}_{}'.format(id, report_name)
        path_list.append(report_path)
    print('path list is ::', path_list)
    return path_list


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

royalty_reports = PythonOperator(
    task_id='fetch_royalty_reports',
    python_callable=get_royalty_reports,
    op_kwargs={'mode' : mode,
               'start_date' : start_date,
               'end_date' : end_date,
               'report_name' : report_name,
               'dag': dag
               },
    dag=dag
)

send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
                'report_list': get_path(),
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> royalty_reports >> send_notification >> end_operator
# start_operator >> royalty_reports >> end_operator
