from distutils.command.config import config
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import airflow
import os
from pickle import TRUE
from tokenize import group
import boto3
import logging
import json
import time
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


default_args = {
    'owner': 'data-axle',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0
}


config_file_path = default_args['currentpath']
with open(config_file_path + '/config.json') as f:
    config = json.load(f)

dag = DAG('datasync_alert_notification',
          default_args=default_args,
          description='Send the alert Mail',
          schedule_interval='@once',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


def datasync_alert_mail(**kwargs):
    connection = boto3.client('datasync', region_name='us-east-1')
    response = connection.list_tasks()
    number_of_task = len(response['Tasks'])
    QueueTaskList = []
    for i in range(number_of_task):
        if response['Tasks'][i]['Status'] == 'QUEUED':
            #when there is no name givem by user for task we are replacing black with no-name
            if len(response['Tasks'][i]) ==2:
                TaskName = "No-Name"
            else:
                TaskName = response['Tasks'][i]["Name"]
            List_Task = connection.list_task_executions(TaskArn=response['Tasks'][i]['TaskArn'])
            task_id = List_Task['TaskExecutions'][-1]['TaskExecutionArn']
            Desc_Task = connection.describe_task_execution(TaskExecutionArn=task_id)
            if Desc_Task['Status'] == 'QUEUED':
                #Latest Execution time of task
                start_time = Desc_Task['StartTime']
                task_id_time = start_time.strftime("%H:%M:%S")
                while(1):
                    time.sleep(30)
                    now = datetime.now()
                    current_time = now.strftime("%H:%M:%S")
                    FMT = '%H:%M:%S'
                    task_duration = datetime.strptime(current_time, FMT)-datetime.strptime(task_id_time, FMT) 
                    task_duration_in_sec = task_duration.total_seconds()
                    if int(task_duration_in_sec) > 5:
                        QueueTaskList.append(TaskName+": "+response['Tasks'][i]['TaskArn'])
                        break
                    else:
                        Desc_Task_Status = connection.describe_task_execution(TaskExecutionArn=task_id)
                        if Desc_Task_Status['Status'] == 'SUCCESS' or Desc_Task_Status['Status'] == 'ERROR':
                            break
                        else:
                            continue

    email_body = "Hi Team,"+"<br><br>"+ "Below are the task are still in queue state in Developement data lake. You can access the task through the link given below.<br><br>"+"Task-Name:      Task-Link<br><br>"
    str1 = "https://console.aws.amazon.com/datasync/home?region=us-east-1#/tasks/"
    task_name_list = []
    for i in QueueTaskList:
        task_id=i.split("/")
        task_desc=i.split(" ")
        task_details=task_desc[0] +" :  "+str1+task_id[1]
        task_name_list.append(task_details)

    task_description = ('<br>'.join(map(str,task_name_list)))
    alrt_mail= email_body + task_description
    
    message = "Hi Team,"+"<br><br>"+ "There is no task in queue state for more than 4 hours in Developement data lake"
    if (len(QueueTaskList) > 0):
        email = EmailOperator(
            mime_charset='utf-8',
            task_id='send_mail_for_alert',
            to= config["to_email_address"],
            subject=config["email_subject"],
            html_content=alrt_mail,
            dag=dag)
        email.execute(context=kwargs)
    else:
        email = EmailOperator(
            mime_charset='utf-8',
            task_id='send_mail_for_alert',
            to= config["to_email_address"],
            subject=config["email_subject"],
            html_content=message,
            dag=dag)
        email.execute(context=kwargs)
        

alert_mail_datasync = PythonOperator(
    task_id='alert_mail',
    python_callable=datasync_alert_mail,
    dag=dag,)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> alert_mail_datasync >> end_operator
