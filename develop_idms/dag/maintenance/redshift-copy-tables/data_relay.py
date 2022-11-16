import datetime
import json
import os
import re
import airflow
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers.send_email import send_email

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure'),
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG('maintenance-redshift-copy-tables',
          default_args=default_args,
          description='copy redshift table from one cluster/env to another',
          schedule_interval=None,
          max_active_runs=1
          )


def data_relay(config_file, **kwargs):
    # Checking for redshift connection
    # read argument value and initialize variables
    tables_copied_dict = {}
    full_schema_dump = 'false'
    source_rs_conn_id = kwargs['dag_run'].conf.get('copy-info')['src']
    dest_rs_conn_id = kwargs['dag_run'].conf.get('copy-info')['dest']
    full_schema_dump = kwargs['dag_run'].conf.get('copy-info')['full-schema']

    logging.info('source connection is ::: ', source_rs_conn_id)
    logging.info('dest connection is ::: ', dest_rs_conn_id)
    logging.info('full_schema_dump flag is ::: ', full_schema_dump)

    # Validate command line arguments
    if full_schema_dump == 'false' and len(kwargs['dag_run'].conf) < 2:
        raise ValueError('copy info and tables list to copy needed as command line arguments')
    elif full_schema_dump == 'true' and len(kwargs['dag_run'].conf) < 1:
        raise ValueError('copy info needed as command line arguments')

    # loading config files to get additional info
    with open(config_file) as f:
        config = json.load(f)

    s3_base_path = config['s3_base_path']
    iam_role = config['iam_role']
    table_owner = config['tbl_owner']

    un_date = str(datetime.date.today())

    if full_schema_dump == 'true':
        table_list_query = ''' SELECT table_schema, table_name
         FROM information_schema.TABLES
         WHERE table_type = 'BASE TABLE' 
         AND table_schema = 'public'  '''
        rs_table_list = get_query_result(source_rs_conn_id, table_list_query, full_schema_dump)
        table_list = ['.'.join(table) for table in rs_table_list]
    else:
        table_list = kwargs['dag_run'].conf.get('tables-to-copy')

    for idx, table in enumerate(table_list):
        unload_sql = 'select * from {}'.format(table)

        unload_query = ''' UNLOAD ('{}') TO '{}/{}/{}/' IAM_ROLE '{}' PARQUET ALLOWOVERWRITE ; '''.format(unload_sql,
                                                                                           s3_base_path,
                                                                                           table,
                                                                                           un_date,
                                                                                           iam_role)

        generate_ddl_query = 'SHOW TABLE {} ;'.format(table)
        grant_access_query = 'ALTER TABLE {} owner to {};'.format(table,
                                                                  table_owner)

        copy_query = ''' COPY {} FROM '{}/{}/{}/' IAM_ROLE '{}' PARQUET ;'''.format(table,
                                                                                    s3_base_path,
                                                                                    table,
                                                                                    un_date,
                                                                                    iam_role)

        logging.info('Unload of data starting to S3 with query ::: {}'.format(unload_query))
        unload_to_s3(source_rs_conn_id, unload_query)

        logging.info('Creating table definition in destination redshift with query ::: {}'.format(generate_ddl_query))
        create_table_query = str(get_query_result(source_rs_conn_id, generate_ddl_query, 'false'))

        ''' To grant/change owner of the table user airflow_app must have sufficient privileges '''
        # final_ddl_query = create_table_query + grant_access_query
        final_ddl_query = create_table_query
        logging.info('create ddl query is ::: {}', final_ddl_query)

        logging.info('starting copy command with query ::: {}'.format(copy_query))
        copy_to_s3(dest_rs_conn_id, final_ddl_query, copy_query)

        # update tables_copied_dict
        tables_copied_dict[idx] = table
        logging.info('Table copied stats ::: {}'.format(tables_copied_dict))

    logging.info('Data Relay completed Successfully')


def unload_to_s3(rs_connection, unload_script):
    redshift_conn_id = Variable.get(rs_connection)
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    logging.info('unloading data from redshift to s3 with script :: ', unload_script)
    redshift_hook.run(unload_script, autocommit=True)


def get_query_result(rs_connection, ddl_script, fs_flag):
    redshift_conn_id = Variable.get(rs_connection)
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(ddl_script)
    if fs_flag == 'true':
        sources = cursor.fetchall()
    else:
        sources = re.sub("(\[\(\')|(\\\\n)|(\'\,\)\])|(\[\(\")|(\"\,\)\])", ' ', str(cursor.fetchall()))

    return sources


def copy_to_s3(rs_connection, create_table_query, copy_script):
    redshift_conn_id = Variable.get(rs_connection)
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    logging.info('Creating table in destination redshift with query :: ', create_table_query)
    redshift_hook.run(create_table_query, autocommit=True)

    logging.info('Copying data from s3 to redshift with query :: ', copy_script)
    redshift_hook.run(copy_script, autocommit=True)


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

data_unload = PythonOperator(
    task_id='data_relay',
    python_callable=data_relay,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag
)

# send success email
send_notification = PythonOperator(
    task_id='email_notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    provide_context=True,
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> data_unload >> send_notification >> end_operator
# start_operator >> data_unload >> end_operator
