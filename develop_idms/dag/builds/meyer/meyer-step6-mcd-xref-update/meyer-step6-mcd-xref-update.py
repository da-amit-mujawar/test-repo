import logging
import os

import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers.redshift import *
from helpers.s3 import move_file
from helpers.send_email import send_email
from helpers.sqlserver import *
from operators import RedshiftOperator, DataCheckOperator


def getbuildlist(databaseid):
    lv_sql = "  SELECT STUFF((SELECT ',' + LTRIM(STR(ID))  " \
             "    FROM dw_admin.dbo.tblOrder " \
             "   WHERE BuildID IN (SELECT ID  FROM dw_admin.dbo.tblBuild  WHERE DatabaseID = {0}) " \
             "     AND dShipDateShipped is not null " \
             "     AND iIsNoUsage = 0  " \
             "     FOR XML PATH('')), 1, 1, '') as OrderList " \
             " FROM dw_admin.dbo.tblDatabase" \
             " WHERE ID = {0}".format(databaseid)
    sqlhook = get_sqlserver_hook()
    build_info = sqlhook.get_first(lv_sql)
    return build_info[0]


def update_count_tables(**kwargs):
    ls_orders = kwargs['ti'].xcom_pull(task_ids='get_order_list')
    db_conn = PostgresHook(postgres_conn_id=Variable.get('var-redshift-postgres-conn'))
    for order_id in ls_orders.split(','):
        logging.info("Update Trans/Promo IDs for Order {0}".format(order_id))
        db_conn.run("call Upd_Meyer_Counttables('{0}')".format(order_id))


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'databaseid': 1352,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

ln_dbid = default_args['databaseid']

dag = DAG('builds-meyer-step6-mcd-xref-update',
          default_args=default_args,
          description='Meyer XRef Update for Transactions and Promotions Data ',
          tags=['prod'],
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# fetch_buildinfo = PythonOperator(task_id="fetch_build",
#                                  python_callable=get_build_info,
#                                  op_kwargs={'databaseid': ln_dbid, 'active_flag': 1},
#                                  provide_context=True,
#                                  dag=dag)

get_orderList = PythonOperator(task_id="get_order_list",
                               python_callable=getbuildlist,
                               op_kwargs={'databaseid': default_args['databaseid']},
                               provide_context=True,
                               dag=dag)

update_count_tables = PythonOperator(task_id='update_count_tables',
                                     python_callable=update_count_tables,
                                     op_kwargs={'databaseid': default_args['databaseid']},
                                     provide_context=True,
                                     dag=dag)

send_notification = PythonOperator(task_id='email-notification',
                                   python_callable=send_email,
                                   op_kwargs={
                                       'config_file': default_args['currentpath'] + '/config.json',
                                       'dag': dag
                                   },
                                   dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> get_orderList >> update_count_tables >> send_notification >> end_operator
