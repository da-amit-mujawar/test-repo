import io
import json
import os

import airflow
import boto3
import pandas as pd
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from helpers.excel import *
# from helpers.redshift import get_redshift_hook
from helpers.s3 import create_presigned_url, copy_file, delete_file
from helpers.send_email import send_email_without_config

default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'current_path': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email': Variable.get('var-email-on-failure-datalake'),
    'email_on_failure': True,
    'email_on_retry': False
}

# redshift_hook = get_redshift_hook()
# open config file
config_file = default_args['current_path'] + '/config.json'
with open(config_file) as f:
    config = json.load(f)
redshift_conn_id = Variable.get('var-redshift-da-rs-01-connection')
redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)


@dag(
    dag_id='email-hygiene-step2',
    default_args=default_args,
    description='Update ehyg tables using csv file returned by oversight and generate report',
    schedule_interval='@once',
    max_active_runs=1
)
def pipeline():
    @task()
    def update_ehyg_table():
        # read argument value
        dag_run_conf = get_current_context()["dag_run"].conf
        if dag_run_conf.get('bucket_name') is None or dag_run_conf.get('file_key') is None:
            raise ValueError('Please pass bucket_name,file_key argument')
        else:
            bucket_name = dag_run_conf.get('bucket_name')
            file_key = dag_run_conf.get('file_key')
        # read the sql file
        scriptfilename = default_args['current_path'] + '/010_update_ehyg_table.sql'
        with open(scriptfilename, 'r') as filehandle:
            strScript = filehandle.read()
        # Extract DB Id from File name
        source_file_name = file_key.split('/')[-1]
        dbid = source_file_name.split('_')[2]
        # replace variables with values in sql file
        strScript = strScript.replace('{bucket_name}', bucket_name)
        strScript = strScript.replace('{file_key}', file_key)
        strScript = strScript.replace('{dbid}', dbid)
        strScript = strScript.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
        redshift_hook.run(strScript, autocommit=True)
        return source_file_name

    @task()
    def unload_data(source_file_name):
        config_updated = {}
        dbid = source_file_name.split('_')[2]
        # read the sql file
        scriptfilename = default_args['current_path'] + '/020_unload_data.sql'
        with open(scriptfilename, 'r') as filehandle:
            strScript = filehandle.read()
        for item in config:
            strScript = strScript.replace('{' + item + '}', str(config[item]))
        # Extract DB Id from File name
        dbid, source_date = source_file_name.split('_')[2:4]
        # replace variables with values in sql file
        strScript = strScript.replace('{dbid}', dbid)
        strScript = strScript.replace("{yyyy}{mm}{dd}", source_date)
        strScript = strScript.replace('{iam}', Variable.get('var-password-redshift-iam-role'))
        # Updating params in config
        config_updated['report_key'] = config['report_key'].replace('{dbid}', dbid)
        config_updated['suppresions_fkey'] = config['suppresions_fkey'].replace(
            '{dbid}', dbid).replace("{yyyy}{mm}{dd}", source_date)
        config_updated['idms_fkey'] = config['idms_fkey'].replace(
            '{dbid}', dbid)
        # Execute SQL script
        redshift_hook.run(strScript, autocommit=True)
        bucket_name = config['unload_bucketname']
        # Generate presigned urls for suppresions and idms file valid for 48 hrs
        url1 = create_presigned_url(bucket_name, config_updated['suppresions_fkey']+'000', 172800)
        url2 = create_presigned_url(bucket_name, config_updated['idms_fkey']+'000', 172800)
        # Return Kwargs to be read by the next task
        return_kwargs = {'dbid': dbid, 'url1': url1, 'url2': url2}
        return return_kwargs

    @task()
    def generate_report(kwargs):
        dbid = kwargs['dbid']
        # Execute SQL which creates the Report tables
        report_key = config['report_key'].replace('{dbid}', dbid)
        scriptfilename = default_args['current_path'] + '/030_generate_report.sql'
        with open(scriptfilename, 'r') as filehandle:
            strScript = filehandle.read()
        strScript = strScript.replace('{dbid}', dbid)
        redshift_hook.run(strScript, autocommit=True)
        # Fetch data from each report table and write to its respective excel sheets
        df1 = redshift_hook.get_pandas_df(f'SELECT * FROM ehyg_{dbid}_report1')
        df2 = redshift_hook.get_pandas_df(f'SELECT * FROM ehyg_{dbid}_report2')
        df3 = redshift_hook.get_pandas_df(f'SELECT * FROM ehyg_{dbid}_report3')
        df4 = redshift_hook.get_pandas_df(f'SELECT * FROM ehyg_{dbid}_report4')
        df5 = redshift_hook.get_pandas_df(f'SELECT * FROM ehyg_{dbid}_report5')
        df6 = redshift_hook.get_pandas_df(f'SELECT * FROM ehyg_{dbid}_report6')
        with io.BytesIO() as output:
            with pd.ExcelWriter(output) as writer:
                # Write each DF to new sheet
                df6.to_excel(writer, sheet_name="suppression_count", index=False)
                df1.to_excel(writer, sheet_name="engaged_counts", index=False)
                df2.to_excel(writer, sheet_name="eo_validation_status", index=False)
                df3.to_excel(writer, sheet_name="eo_email_domain_group", index=False)
                df4.to_excel(writer, sheet_name="ehygiene_code_counts", index=False)
                df5.to_excel(writer, sheet_name="idms_ehyg_code_counts", index=False)
                # Set column width to max value length of each column
                set_column_width(df6, "suppression_count", writer)
                set_column_width(df1, "engaged_counts", writer)
                set_column_width(df2, "eo_validation_status", writer)
                set_column_width(df3, "eo_email_domain_group", writer)
                set_column_width(df4, "ehygiene_code_counts", writer)
                set_column_width(df5, "idms_ehyg_code_counts", writer)
            data = output.getvalue()
        # Generate Presigned URL for the Excel report
        bucket_name = config['unload_bucketname']
        s3 = boto3.resource("s3")
        s3.Bucket(bucket_name).put_object(Key=report_key, Body=data)
        report_url = create_presigned_url(bucket_name, report_key, 172800)
        kwargs['url3'] = report_url
        return kwargs

    @task(retries=2)
    def send_notification(kwargs):
        dbid = kwargs['dbid']
        url1 = kwargs['url1']
        url2 = kwargs['url2']
        url3 = kwargs['url3']
        email_message = config['email_message'].replace(
            '{url1}', url1).replace('{url2}', url2).replace('{url3}', url3)
        email_subject = config['email_subject'].replace('{dbid}', dbid)
        send_email_without_config(
            dag=dag,
            email_address=config['email_address'],
            email_business_users=config['email_business_users'],
            email_subject=email_subject,
            email_message=email_message
        )
        return kwargs['dbid']

    @task()
    def cleanup_resources(dbid):
        # Execute SQL which will drop all Step 1 and 2 tables
        scriptfilename = default_args['current_path'] + '/040_cleanup.sql'
        with open(scriptfilename, 'r') as filehandle:
            strScript = filehandle.read()
        strScript = strScript.replace('{dbid}', dbid)
        redshift_hook.run(strScript, autocommit=True)
        # Move input file to Processed folder
        dag_run_conf = get_current_context()["dag_run"].conf
        input_bucket_name = dag_run_conf.get('bucket_name')
        input_file_key = dag_run_conf.get('file_key')
        input_fkey_split = input_file_key.split('/')
        if len(input_fkey_split) > 1:
            dest_file_key = '/'.join(input_fkey_split[:-1]) + '/processed/' + input_fkey_split[-1]
        else:
            dest_file_key = 'processed/' + input_file_key
        copy_file(input_bucket_name, input_bucket_name+'/'+input_file_key, dest_file_key)
        delete_file(input_bucket_name, input_file_key)

    source_file_name = update_ehyg_table()
    return_kwargs1 = unload_data(source_file_name)
    return_kwargs2 = generate_report(return_kwargs1)
    dbid = send_notification(return_kwargs2)
    cleanup_resources(dbid)


ehyg_s2_dag = pipeline()
