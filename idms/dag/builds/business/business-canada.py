import os
import logging
import boto3
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mssql_operator import MsSqlOperator
from operators import RedshiftOperator, DataCheckOperator
from helpers.send_email import send_email
from helpers.sqlserver import *
from helpers.redshift import *
from time import *

default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 1293,
    "retries": 0,
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
    "email_on_retry": False,
}

# Set ETL RedShift Connection
# Cluster must be started and stopped manually until automation is in place
redshift_etl_connection = Variable.get("var-redshift-etl-conn")
redshift_connection = Variable.get("var-redshift-postgres-conn")

dag = DAG(
    "builds-business-canada",
    default_args=default_args,
    description="builds business",
    schedule_interval="@once",
    tags=["prod"],
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# cluster resume function
def cluster_resume():
    REDSHIFT_CLUSTER_ID = "da-idms-etl-01"
    try:
        redshift_client = boto3.client("redshift", region_name="us-east-1")
        response = redshift_client.describe_clusters(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID
        )
        if response["Clusters"][0]["ClusterStatus"] == "paused":
            redshift_client.resume_cluster(ClusterIdentifier=REDSHIFT_CLUSTER_ID)
            logger.info(
                "redshift cluster {0} is now resuming".format(REDSHIFT_CLUSTER_ID)
            )
        else:
            logger.info(
                "redshift cluster {0} is in a {1} state. cannot resume cluster".format(
                    REDSHIFT_CLUSTER_ID, response["Clusters"][0]["ClusterStatus"]
                )
            )

        response_modify = redshift_client.describe_clusters(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID
        )
        while response_modify["Clusters"][0]["ClusterStatus"] != "available":
            response_modify = redshift_client.describe_clusters(
                ClusterIdentifier=REDSHIFT_CLUSTER_ID
            )

        logger.info("redshift cluster {0} is available".format(REDSHIFT_CLUSTER_ID))

    except Exception as e:
        logger.error("Exception: {0}".format(e))


# cluster Pause function
def cluster_pause():
    REDSHIFT_CLUSTER_ID = "da-idms-etl-01"
    try:
        redshift_client = boto3.client("redshift", region_name="us-east-1")
        response = redshift_client.describe_clusters(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID
        )
        if response["Clusters"][0]["ClusterStatus"] == "available":
            redshift_client.pause_cluster(ClusterIdentifier=REDSHIFT_CLUSTER_ID)
            logger.info(
                "redshift cluster {0} is now paused".format(REDSHIFT_CLUSTER_ID)
            )
        else:
            logger.info(
                "redshift cluster {0} is in a {1} state. cannot pause cluster".format(
                    REDSHIFT_CLUSTER_ID, response["Clusters"][0]["ClusterStatus"]
                )
            )
    except Exception as e:
        logger.error("Exception: {0}".format(e))


resume_cluster = PythonOperator(
    task_id="resume_cluster",
    python_callable=cluster_resume,
    # provide_context=True,
    dag=dag,
)

fetch_buildinfo = PythonOperator(
    task_id="fetch-build",
    python_callable=get_build_info,
    op_kwargs={"databaseid": default_args["databaseid"]},
    provide_context=True,
    dag=dag,
)

pause_cluster = PythonOperator(
    task_id="pause_cluster",
    python_callable=cluster_pause,
    # provide_context=True,
    dag=dag,
)

# run sql scripts
data_load1 = RedshiftOperator(
    task_id="load-company-data",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/010-1-*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)


data_load2 = RedshiftOperator(
    task_id="load-individual-data",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/010-2-*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load4 = RedshiftOperator(
    task_id="create-business-individual-table",
    dag=dag,
    sql_file_path=default_args["current_path"]
    + "/010-create-busscompany-individual-table.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

parallel_task1 = [data_load1, data_load2, data_load4]
#parallel_task1 = [ data_load4]
delete_usa_records = RedshiftOperator(
    task_id="delete-usa-records",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-011-delete-usa-records.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load5 = RedshiftOperator(
    task_id="insert-into-businessindividual",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/012*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load6 = RedshiftOperator(
    task_id="update-businessindividual",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-013-1-update-tblbusinessindividual.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)


data_load7 = RedshiftOperator(
    task_id="load-application-segment",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-014-4-load-application-segment.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load8 = RedshiftOperator(
    task_id="application-tables",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-015-1-*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load9 = RedshiftOperator(
    task_id="segment-tables",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-015-2-*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load10 = RedshiftOperator(
    task_id="ddvalues-1-tables",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/015-3*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load11 = RedshiftOperator(
    task_id="ddvalues-2-tables",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-015-4-DDValues-part2.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load12 = RedshiftOperator(
    task_id="ddvalues-3-tables",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/015-5*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load13 = RedshiftOperator(
    task_id="create-3rd-tables",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/015-6*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

parallel_task2 = [
    data_load8,
    data_load9,
    data_load10,
    data_load11,
    data_load12,
    data_load13,
]

data_load14 = RedshiftOperator(
    task_id="all-combined-join",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-016-all-combined-joins.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load15 = RedshiftOperator(
    task_id="add-calculation-ctas1",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-017-Add-calculations-1.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load16 = RedshiftOperator(
    task_id="ctas1-update",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-018-update-ctas1.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)

data_load16_1 = RedshiftOperator(
    task_id="export-csv-maintable",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-018-2-export-as-csv.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_etl_connection,
)
data_load17 = RedshiftOperator(
    task_id="create-maintable",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/CA-019-0-create-maintable.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_connection,
)

data_load17_1 = RedshiftOperator(
    task_id="create-view",
    dag=dag,
    sql_file_path=default_args["current_path"] + "/019-1*.sql",
    config_file_path=default_args["current_path"] + "/config.json",
    redshift_conn_id=redshift_connection,
)

get_count = PythonOperator(
    task_id="count_maintable",
    python_callable=countquery,
    op_kwargs={
        "tablename": Variable.get("var-db-1293", deserialize_json=True)["maintable_name"]
    },
    provide_context=True,
    dag=dag,
)

build_activate = PythonOperator(
    task_id="activate_build",
    python_callable=activate_build,
    op_kwargs={
        "database_id": default_args["databaseid"],
        "build_status": 61,
        "min_expected_count": 25000,
        "count_task_id": "count_maintable",
        "sql_conn_id": Variable.get("var-sqlserver-conn"),
    },
    provide_context=True,
    dag=dag,
)

# send success email
send_notification = PythonOperator(
    task_id="email_notification",
    python_callable=send_email,
    op_kwargs={
        "config_file": default_args["current_path"] + "/config.json",
        "dag": dag,
    },
    dag=dag,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)


(
    start_operator
    >> resume_cluster
    >> fetch_buildinfo
    >> parallel_task1
    >> delete_usa_records
    >> data_load5
    >> data_load6
    >> data_load7
    >> parallel_task2
    >> data_load14
    >> data_load15
    >> data_load16
    >> data_load16_1
    >> pause_cluster
    >> data_load17
    >> data_load17_1
    >> get_count
    >> build_activate
    >> send_notification
    >> end_operator
)
