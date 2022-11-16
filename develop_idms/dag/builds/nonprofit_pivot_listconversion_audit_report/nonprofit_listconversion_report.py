import os
import airflow
from airflow.models import Variable
from builds.nonprofit_pivot_listconversion_audit_report.tasks import *
from helpers.send_email import send_email_without_config
from airflow.decorators import dag, task
from helpers.common import prepare_sql_file_using_df
from helpers.redshift import executenonquery

default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
}

@dag(
    default_args=default_args,
    dag_id='nonprofit-pivot-listconversion-audit-report',
    description='Non Profit Audit Report',
    schedule_interval='@once',
    max_active_runs=1
)


def pipeline():
    @task
    def generate_report(**kwargs):
        generate_report = generateAuditReport(**kwargs)
        return generate_report


    @task()
    def send_email(**kwargs):
        if kwargs["dag_run"].conf.get("databaseid") == None:
            raise ValueError("Please pass DatabaseID")
        else:
            DatabaseID = kwargs["dag_run"].conf.get("databaseid")
        
        send_email_without_config (
            email_business_users=["ApogeeUpdateTeam@data-axle.com", "dl-donorbaseupdateteam@data-axle.com"],
            email_address="DWOrders@data-axle.com",
            report_list= [f"/tmp/Nonprofit_Pivot_AuditReport_{DatabaseID}.xlsx"],
            dag=dag,
            email_subject="IDMS non profit Audit Report",
            email_message="Hello IDMS-Admin Team,<br/><br/> audit report has been generated, please find attached Audit Report"
        )

    

    (
        generate_report() >> 
        send_email() 
    )

nonprofit_listconversion_audit_report = pipeline()
