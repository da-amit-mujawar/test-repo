import os
import airflow
from airflow import DAG
from airflow.models import Variable
from helpers.send_email import send_email_without_config
from datetime import datetime
from helpers.sqlserver import *
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
from contextlib import closing
default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
    "email_on_retry": False,
}
sqlhook = get_sqlserver_hook()
sql_conn = sqlhook.get_conn()
sql_conn.autocommit(True)
s3_internal =  Variable.get("var-s3-bucket-names", deserialize_json=True)["s3-internal"]
time_stamp = datetime.today().strftime("%Y%m%d")
DatabaseID =  0
ChangePercentage = 0
dag = DAG('DBBuild-Fill-Rate-Report',
          default_args=default_args,
          description="DBBuild-Fill-Rate-Report",
          schedule_interval="@once",
          max_active_runs=1
        )
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
def Fill_rate_QC_report(**kwargs):
    if kwargs["dag_run"].conf.get("databaseid") == None:
        raise ValueError("Please pass DatabaseID")
    else:
        DatabaseID = kwargs["dag_run"].conf.get("databaseid")
    if kwargs["dag_run"].conf.get("changepercentage") == None:
       ChangePercentage = 5
    else:
        ChangePercentage = kwargs["dag_run"].conf.get("changepercentage")
    sql=f"""EXEC Common.dbo.usp_QCBuildReport @DatabaseID = {DatabaseID}, @ChangePercentage  = {ChangePercentage} """,
    sqlhook.run(sql)
    
def output_qc_report_with_percentage(**kwargs):
    if kwargs["dag_run"].conf.get("databaseid") == None:
        raise ValueError("Please pass DatabaseID")
    else:
        DatabaseID = kwargs["dag_run"].conf.get("databaseid")
    file_output_path_error = f'/tmp/{DatabaseID}_Error_{time_stamp}.xlsx'    
    sql = f"""SELECT * 
            FROM Tempdata.dbo.tbl_QCBuildReport_{DatabaseID}_Error
            Where databaseid={DatabaseID}
            and qc_date= CAST(GETDATE() AS DATE)
            Order by ChangePercentage desc"""
    qc_report_with_percentagedf = sqlhook.get_pandas_df(sql)
    print(qc_report_with_percentagedf.to_markdown())
    writer = pd.ExcelWriter(file_output_path_error)
    qc_report_with_percentagedf.to_excel(writer, sheet_name="qc_report_with_percentage")
    writer.save()
    
def output_qc_report_with_fields(**kwargs):
    if kwargs["dag_run"].conf.get("databaseid") == None:
        raise ValueError("Please pass DatabaseID")
    else:
        DatabaseID = kwargs["dag_run"].conf.get("databaseid")
    file_output_path_fields = f'/tmp/{DatabaseID}_Fields_{time_stamp}.xlsx'
    builddf = get_build_df(databaseid = DatabaseID, active_flag = 1) 
    Buildid = builddf['build_id'][0]
    sql = f"""SELECT TOP 5000  
                ID,
                DatabaseID,
                BuildID,
                ListID,
                [Field Name],
                [Field Value],
                Description,
                PrevCount,
                CurrCount,
                ChangeDifference,
                ChangePercentage 
                FROM common.dbo.tbl_QCBuildReport
                Where DatabaseID={DatabaseID} 
                And buildid={Buildid}"""
    qc_report_with_fieldsdf = sqlhook.get_pandas_df(sql)
    print(qc_report_with_fieldsdf.to_markdown())
    writer1 = pd.ExcelWriter(file_output_path_fields)
    qc_report_with_fieldsdf.to_excel(writer1, sheet_name="qc_report_with_fields")
    writer1.save()
  
def send_email(**kwargs):
    if kwargs["dag_run"].conf.get("databaseid") == None:
        raise ValueError("Please pass DatabaseID")
    else:
        DatabaseID = kwargs["dag_run"].conf.get("databaseid")
    with closing(sql_conn.cursor()) as sql_cursor:
      sql = f"""SELECT TOP 1 cadministratoremail FROM dw_admin.dbo.tblDatabase
                  WHERE ID = {DatabaseID}"""
      # db_admin_email_df = sqlhook.get_pandas_df(sql)
      # print(db_admin_email_df)
      sql_cursor.execute(sql)
      row_sql_email = sql_cursor.fetchone()
      print(row_sql_email)
      
    email_business_users =[]
    email_business_users.append((f"{row_sql_email}"))
    print(email_business_users)
    
    send_email_without_config(
                report_list = [f"/tmp/{DatabaseID}_Error_{time_stamp}.xlsx",
                               f"/tmp/{DatabaseID}_Fields_{time_stamp}.xlsx"],
                email_business_users = email_business_users,
                email_address= ["DWorders@infogroup.com","triyanshi.gupta@data-axle.com"],
                email_subject=f"Review Fill Rates QC Report {DatabaseID}",
                email_message="Please find attached Report.")
    
QC_report = PythonOperator(
  task_id="Fill_rate_QC_report",
  python_callable=Fill_rate_QC_report,
  provide_context=True,
  dag=dag)
qc_report_with_percentage = PythonOperator(
  task_id="output-qc-report-with-percentage",
  python_callable=output_qc_report_with_percentage,
  provide_context=True,
  dag=dag)
qc_report_with_fields = PythonOperator(
  task_id="output-qc-report-with-fields",
  python_callable=output_qc_report_with_fields,
  provide_context=True,
  dag=dag)
send_notification = PythonOperator(
    task_id='email_notification',
    python_callable= send_email,
    provide_context=True,
    dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)
start_operator  >> QC_report >> qc_report_with_percentage >> qc_report_with_fields >> send_notification >> end_operator