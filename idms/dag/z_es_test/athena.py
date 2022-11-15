import os
from os.path import dirname, abspath
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import awswrangler as wr
import boto3
import datetime
import pandas as pd
from concurrent import futures
import billiard as multiprocessing
#from billiard import Pool, cpu_count

default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "retries": 0,
    "email_on_retry": False,
}

dag = DAG(
    "athena-parallel-execution",
    default_args=default_args,
    description="athena-parallel-execution",
    schedule_interval="@once",
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

# redshift connection
redshift_conn_id = Variable.get("var-redshift-da-rs-01-connection")
redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
athena_temp_bucket = "s3://idms-2722-athena/" 
athena_database = "idms"

boto3.setup_default_session(region_name="us-east-1")
DatabaseID = 0
BuildID = 0

def get_athena_donor_counts(DonorTableName):
    df_athena1 = wr.athena.read_sql_query(
        sql=f"""SELECT COUNT(*) as idonor FROM {DonorTableName}""",
        database=athena_database,
        s3_output=athena_temp_bucket,
        use_threads = True
    )
    return df_athena1

def fetch_last_inserted_id(BuildID, redshift_hook):
    redshift_sql2 = f"""select max(id) as id from es_Exclude_ListConversion_Audit WHERE BuildID = {BuildID}"""
    return redshift_hook.get_pandas_df(redshift_sql2)

def export_data(**kwargs):
    def parallel_process_ownerid(buildid):
        DonorTableName = ( f"DW_Final_961_{buildid}_11305")
        print(f"DonorTableName = {DonorTableName}")
        redshift_sql1 = f"""DELETE FROM es_Exclude_ListConversion_Audit WHERE BuildID = {buildid};
                            INSERT INTO es_Exclude_ListConversion_Audit (DatabaseID, BuildID, ListOwnerID, ListID, dCreatedDate) VALUES 
                        (961,{buildid},1,11305, GETDATE());"""
        print(f"redshift_sql1 = {redshift_sql1}")
        redshift_hook.run(redshift_sql1, autocommit=True)

        ID = fetch_last_inserted_id(buildid, redshift_hook)
        print(f"ID = {ID}")
        df_athena1 = get_athena_donor_counts(DonorTableName)
        iDonor = df_athena1["idonor"].values[0]
        print(f"iDonor = {iDonor}")

        df_athena3 = wr.athena.read_sql_query(
                            sql=f"""SELECT COUNT(listid) AS itransaction FROM {DonorTableName}""",
                            database=athena_database,
                            s3_output=athena_temp_bucket,
                            use_threads = True
                        )

        iTransaction = df_athena3["itransaction"].values[0]
        print(f"iTransaction = {iTransaction}")
        redshift_sql2 = f"""UPDATE es_Exclude_ListConversion_Audit
                            SET	iDonor = {iDonor},
                            iTransaction = {iTransaction}
                            WHERE ID = {ID["id"].values[0]}"""
        redshift_hook.run(redshift_sql2, autocommit=True)
    
    
    list_of_buildid = [20683,20684,20685,20686,20687,20688,20689,20690,
                        20691,20692,20693,20694,20695,20696,20697,20698,20699,
                        20650,20651,20652,20653,20654,20655,20656,20657,20658,20659,20660,
                        20661,20662,20663,20664,20665,20666,20667,20668,20669,20670,
                        20671,20672,20673,20674,20675,20676,20677,20678,20679,20680,20681,20682]
    for buildid in list_of_buildid:
        parallel_process_ownerid(buildid)


# def export_data():
#     list_of_buildid = [20683,20684,20685,20686,20687,20688,20689,20690,
#                         20691,20692,20693,20694,20695,20696,20697,20698,20699,
#                         20650,20651,20652,20653,20654,20655,20656,20657,20658,20659,20660,
#                         20661,20662,20663,20664,20665,20666,20667,20668,20669,20670,
#                         20671,20672,20673,20674,20675,20676,20677,20678,20679,20680,20681,20682]
#     pool = multiprocessing.Pool()
#     pool.map(parallel_process_ownerid, list_of_buildid)
#     pool.close()

    # with futures.ThreadPoolExecutor() as executor:
    #     result = executor.map(parallel_process_ownerid, list_of_buildid)

export_data = PythonOperator(
    task_id="export_data",
    python_callable=export_data,
    provide_context=True,
    dag=dag,
)
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> export_data >> end_operator