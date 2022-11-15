from airflow.models import Variable
from datetime import datetime
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp
import json
import boto3
import os.path
import os
import logging

def send_email(config_file, dag, report_list=[],**kwargs):
    with open(config_file) as f:
        config = json.load(f)

    notification_email = []
    if 'email_address' in config:
        notification_email = [Variable.get(config['email_address'])]

    if not report_list:
        s3_object = boto3.client('s3', 'us-east-1')
        bucket_name = Variable.get('var-report-bucket-name')

        report_list = []
        file_ext ='.csv'
        for item in config:
            if 'reportname' in item:
                key = config[item][1:] + '000'
                file_name = os.path.basename(key)
                logging.info(f'Report is being downloaded: {file_name}')
                tmp_file_name = f'/tmp/{file_name[:-3]}{file_ext}'
                if os.path.exists(tmp_file_name):
                    os.remove(tmp_file_name)
                try:
                    s3_object.download_file(bucket_name, key, tmp_file_name)
                except:
                    logging.info(f'Error downloading report: {tmp_file_name}')
                    continue
                report_list.append(tmp_file_name)
                logging.info(tmp_file_name)

        logging.info(report_list)
    #if report list is provided in the parameter
    else:
        new_report_list = []
        s3_object = boto3.client('s3', 'us-east-1')
        for report in report_list:
            #if path to s3 report is provided bucket and key need to be parsed
            if 's3:' in report:
                bucket_name = report[5:][:report[5:].find('/')]
                key = report[5:][report[5:].find('/'):]
                file_name = os.path.basename(key)
                logging.info(f'Report is being downloaded: {file_name}')
                tmp_file_name = f'/tmp/{file_name}'
                if os.path.exists(tmp_file_name):
                    os.remove(tmp_file_name)
                try:
                    s3_object.download_file(bucket_name, key, tmp_file_name)
                except:
                    logging.info(f'Error downloading report: {tmp_file_name}')
                    continue
                new_report_list.append(tmp_file_name)
                logging.info(tmp_file_name)
            # assume report is created on airflow server
            else:
                new_report_list.append(report)
        report_list = new_report_list

    email = EmailOperator(
        mime_charset='utf-8',
        task_id='email_task',
        to=[Variable.get('var-email-on-success')],
        cc=notification_email,
        subject=config['email_subject'],
        html_content=config['email_message'],
        files=report_list,
        dag=dag)
    email.execute(context=kwargs)
