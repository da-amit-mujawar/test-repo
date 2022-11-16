import os
import sys
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from helpers.send_email import send_email
import boto3
import boto3.session
import json
from datetime import datetime
import logging
import time

# jira: 992  Reju Mathew 2021.0215

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


def countdictvalues(d):
    count = 0
    try:
        for x in d:
            if isinstance(d[x], list):
                count += len(d[x])
    except:
        logging.info(f"unable to complete func count job dictvalues. counted: {count}")
    return count


def deletegluejob(config_file, **kwargs):
    with open(config_file) as f:
        config = json.load(f)

    daystokeep = int(config['deletejobsolderthan'])
    sessionprofilename = config['sessionprofilename']
    excludelist = list(config['Excludejobs'].upper().split(','))
    sleeptime = int(config['sleeptime'])

    # some validation to avoid delete all the jobs
    if daystokeep < 7:
        daystokeep = 7

    # log the details
    logging.info(f"Delete jobs older than {daystokeep}")
    logging.info(f"Exclude list:")
    logging.info(excludelist)

    # pass the profile name or let system pick it.
    if sessionprofilename.strip() != '':
        session = boto3.session.Session(profile_name=sessionprofilename)
        glueClient = session.client('glue', region_name=config['region'])
    else:
        glueClient = boto3.client('glue', region_name=config['region'])

    response = glueClient.get_jobs(MaxResults=1000)
    logging.info(f"Total jobs to check = {countdictvalues(response)}")

    # loop through response for each job. Exclude jobName stored in config file dictionary
    intjobstodelete = 0
    for jobs in response.get('Jobs'):
        # print (jobs)
        jobname = jobs['Name']
        lastmodified = jobs['LastModifiedOn']
        noofdays = (datetime.now() - lastmodified.replace(tzinfo=None)).days
        jobname_tosearch = jobname.upper()

        # Enabled the log to see all the jobs.  disable after testing
        # logging.info(f"Jobname: {jobname}  -Modified on - {lastmodified} ")

        if jobname.startswith('IDMS_') and noofdays > daystokeep and jobname.count('_') > 2:
            jobname_left2 = jobname_tosearch.split('_')[0] + '_' + jobname_tosearch.split('_')[1]
            if jobname_left2 not in excludelist and jobname_tosearch not in excludelist:
                glueClient.delete_job(JobName=jobname)
                time.sleep(sleeptime)
                logging.info(f"Delete: {jobname}  -Modified on - {lastmodified} ' - Days: {noofdays}")
                intjobstodelete = intjobstodelete + 1
            else:
                logging.info(
                    f"skipped from delete. (Reason: Excludejobs config) :{jobname}  -Modified on - {lastmodified}  - Days: {noofdays}")
        else:
            # no need of show all in log. "
            logging.info(
                f"skipped from delete (Reason: out of date range or not an idms jobs) : {jobname} -Modified on {lastmodified} '- Days: {noofdays}")

    # finally show the delete count
    logging.info(f"Total glue jobs deleted {intjobstodelete}")


# Dags from here.


dag = DAG('maintenance-delete-old-glue-jobs',
          default_args=default_args,
          description='maintenance delete glue jobs',
          schedule_interval='0 6 * * *',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

deletegluejob = PythonOperator(
    task_id='delete_glue_job',
    python_callable=deletegluejob,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag,
)

send_notification = PythonOperator(
    task_id='email-notification',
    python_callable=send_email,
    op_kwargs={'config_file': default_args['current_path'] + '/config.json',
               'dag': dag
               },
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> deletegluejob >> send_notification >> end_operator
