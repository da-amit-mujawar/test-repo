import os
import airflow
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from helpers.redshift import countquery
from helpers.send_email import send_email,send_email_without_config
from helpers.sqlserver import get_build_df,get_sqlserver_hook


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 74,
    "retries": 0,
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
    "email_on_retry": False,
}

logger = logging.getLogger()
logger.setLevel(logging.INFO)
time_stamp = datetime.today().strftime("%Y%m%d")

@dag(
    default_args=default_args,
    dag_id = "builds-nonprofit-monthly-step5-activate-build",
    description="Updates tblBuild for activation",
    schedule_interval="@once",
    max_active_runs=1,
)

def pipeline():
    def fetch_buildinfo():
        fetch_buildinfo_df = get_build_df(
            databaseid=default_args["databaseid"], active_flag=0
            )
        return fetch_buildinfo_df

    def get_count(fetch_buildinfo_df):
        counts = countquery(
            tablename= fetch_buildinfo_df["maintable_name"][0]
            )
        return counts

    @task
    def build_activate():
        fetch_buildinfo_df = fetch_buildinfo()
        build_status = 70
        sqlhook = get_sqlserver_hook()
        counts = get_count(fetch_buildinfo_df)
        logging.info(f"Main table record count: {str(counts)}")

        if counts < 500000000:
            raise ValueError("Main table record count is too low")

        else:
            logging.info("Updating tblbuild ...")
            activate_sql = f"""
                UPDATE TOP(1) tblBuild SET LK_BuildStatus ={build_status}, iRecordCount ={counts},
                dScheduledDateTime ='{str(datetime.now())[:22]}', dModifiedDate ='{str(datetime.now())[:22]}', cModifiedBy = 'Airflow'
                WHERE ID = {fetch_buildinfo_df['build_id'][0]}
                """
            try:
                logging.info(f"Update query: {activate_sql}")
                sqlhook.run(activate_sql)
            except:
                logging.info("Error updating tblbuild")
                raise ValueError("Error activating the build")

    @task()
    def send_notification():
        fetch_buildinfo_df = fetch_buildinfo()
        send_email_without_config(
            email_business_users=["caroline.burch@data-axle.com"],
            email_subject=f"Non-Profit Build Activated for: {fetch_buildinfo_df['dbid'][0]}-{fetch_buildinfo_df['build_id'][0]}",
            email_message="Hello Team,<br/><br/> The NonProfit Build is complete and staged for activation. </br></br>If you have any questions regarding this build, please open a new support ticket.</br></br></br>Thank you.</br></br>- IDMS Support Team</br>help-develop_idms@data-axle.com",
            dag=dag)


    (
        build_activate()>>
        send_notification()
    )

NonProfit_Activate_Build_dag = pipeline()