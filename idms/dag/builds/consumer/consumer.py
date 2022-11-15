import os
import io
import boto3
import logging
import airflow
from airflow import DAG
from airflow.models import Variable
from helpers.send_email import send_email,send_email_without_config
from helpers.redshift import countquery
from helpers.sqlserver import get_build_df,get_sqlserver_hook
from time import *
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task
from helpers.common import prepare_sql_file_using_df
import pandas as pd
from datetime import date,datetime
from airflow.hooks.postgres_hook import PostgresHook


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "current_path": os.path.abspath(os.path.dirname(__file__)),
    "databaseid": 1267,
    "retries": 0,
    "email": Variable.get("var-email-on-failure"),
    "email_on_failure": True,
    "email_on_retry": False,
}

# Set ETL RedShift Connection
# Cluster must be started and stopped manually until automation is in place
redshift_etl_connection = Variable.get("var-redshift-etl-conn")
redshift_hook_etl = PostgresHook(postgres_conn_id = redshift_etl_connection)
redshift_connection = Variable.get("var-redshift-postgres-conn")
redshift_hook = PostgresHook(postgres_conn_id = redshift_connection)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
time_stamp = datetime.today().strftime("%Y%m%d")

@dag(
    default_args=default_args,
    dag_id="builds-consumer",
    description="builds consumer",
    schedule_interval="@once",
    max_active_runs=1,
)
def pipeline():
    def fetch_buildinfo():
        fetch_buildinfo_df = get_build_df(
            databaseid=default_args["databaseid"], active_flag=0
            )
        return fetch_buildinfo_df

    @task
    def update_sql_file():
        #read the sql file
        scriptfilename = default_args['current_path'] +"/131-5-b2c-fastcount.sql"
        filehandle = open(scriptfilename, 'r')
        strScript = filehandle.read()
        filehandle.close()

        build_info_b2c = get_build_df(databaseid=1150, active_flag=1)
        
        build_info_consumer = get_build_df(databaseid=default_args["databaseid"], active_flag=0)

        if build_info_b2c:
                strScript = strScript.replace('{build_id_b2c}', str(build_info_b2c['build_id'][0]))
                strScript = strScript.replace('{build_b2c}', build_info_b2c['build'][0])

        if build_info_consumer:
                strScript = strScript.replace('{maintable_name}', build_info_consumer['maintable_name'][0])
                strScript = strScript.replace('{build_id}', str(build_info_consumer['build_id'][0]))
                strScript = strScript.replace('{build}', build_info_consumer['build'][0])

        redshift_hook.run(strScript, autocommit=True)

    @task
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

    @task
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

    @task()
    def data_load1():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/01*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load1_1():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/21*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook.run(sql, autocommit=True)


    @task()
    def data_load2():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/02*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load3():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/03*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load3_1():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/300-create-load-flagtable.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    parallel_task1 = [data_load1(), data_load2(), data_load3(),data_load1_1(),data_load3_1()]

    @task()
    def data_load4():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/04*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)
    
    @task()
    def data_load5():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/050-1-DDValues-Load.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load6():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/060-1-mailorderbuyer.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load7():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/070-1-segmentcode.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load8():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/080-1-cellphone-countycode-city-donotcallflag.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    parallel_task2 = [data_load5(), data_load6(), data_load7(), data_load8()]

    @task()
    def combined_child():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/080-2-combine all join tables.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load9():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/120-*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook_etl.run(sql, autocommit=True)

    @task()
    def data_load10():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/121-create-main-table.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook.run(sql, autocommit=True)

    @task()
    def data_load10_1():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/130*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook.run(sql, autocommit=True)

    @task()
    def data_load10_2():
        fetch_buildinfo_df = fetch_buildinfo()
        sql = prepare_sql_file_using_df(
                sql_file=default_args['current_path'] + "/220*.sql",
                fetch_buildinfo=fetch_buildinfo_df,
                config_file=default_args['current_path'] + "/config.json",
                )      
        redshift_hook.run(sql, autocommit=True)

    parallel_task4 = [data_load10_1(),data_load10_2(),update_sql_file()]

    trigger_Fill_rate_QC_report = TriggerDagRunOperator(
        task_id = "trigger_DBBuild-Fill-Rate-Report",
        trigger_dag_id = "DBBuild-Fill-Rate-Report",
        conf = {"databaseid":default_args["databaseid"], "changepercentage":5},
        wait_for_completion = True,
        start_date = airflow.utils.dates.days_ago(2)
    )
    def get_count(fetch_buildinfo_df):
        counts = countquery(
            tablename= fetch_buildinfo_df["maintable_name"][0]
            )
        return counts

    @task
    def build_activate():
        fetch_buildinfo_df = fetch_buildinfo()
        build_status = 61
        sqlhook = get_sqlserver_hook()
        counts = get_count(fetch_buildinfo_df)
        logging.info(f"Main table record count: {str(counts)}")

        if counts < 25000:
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
                logging.info("Error updating tblebuild")
                raise ValueError("Error activating the build")
                
    @task()
    def send_notification():
        send_email_without_config(
        report_list= [f"s3://idms-7933-internalfiles/Reports/DataAxle_Auto_MMDB_Profile_{time_stamp}_Infutor.xlsx",
                    f"s3://idms-7933-internalfiles/BuildAuditReports/InfogroupConsumer_1267_Sample000",
                    f"s3://idms-7933-internalfiles/Reports/AuditReport_AutoLinkTable000",
                    f"s3://idms-7933-internalfiles/Reports/auto_IDMS_extract000"],
        email_business_users = ["idmsadminconsumerpearl@data-axle.com"],
        email_subject= f"Consumer File in IDMS (1267) Update",
        email_message= "Hi All,</br></br> Consumer Database (1267) is ready.</br></br> Please review the attached sample and QC counts via IDMS applicatio.</br></br> Once verified you may activate the build. </br></br> Please open a new support ticket for any questions regarding this update.",
        dag = dag)
        

    send_notification.post_execute = lambda **x: sleep(600)

    @task
    def write_excel():
        redshift_sql1 = f"select make as Make,COUNT(*) from auto_mmdb_infutor group by make order by make"
        make_df = redshift_hook.get_pandas_df(redshift_sql1)
        make_df.name = 'Make'
        
        redshift_sql2 = f"""select bodytype as "Body Type",COUNT(*) from auto_mmdb_infutor group by bodytype order by bodytype"""
        bodytype_df = redshift_hook.get_pandas_df(redshift_sql2)
        bodytype_df.name = 'Body type'

        redshift_sql3 = f"select drive_type as Drive_Type,COUNT(*) from auto_mmdb_infutor group by drive_type order by drive_type"
        drivetype_df = redshift_hook.get_pandas_df(redshift_sql3)
        drivetype_df.name = 'Drive type'
        
        redshift_sql4 = f"""select vehtype as "Vehicle Type",COUNT(*) from auto_mmdb_infutor group by vehtype order by vehtype"""
        vehtype_df = redshift_hook.get_pandas_df(redshift_sql4)
        vehtype_df.name = 'Vehicle type'
        
        redshift_sql5 = f"""select luxnon as "Non-Luxury/Luxury" ,COUNT(*) from auto_mmdb_infutor group by luxnon order by luxnon"""
        luxnon_df = redshift_hook.get_pandas_df(redshift_sql5)
        luxnon_df.loc[luxnon_df["non-luxury/luxury"]  == "N", "non-luxury/luxury"] = "Non-Luxury"
        luxnon_df.loc[luxnon_df["non-luxury/luxury"] == "Y",  "non-luxury/luxury"] = "Luxury"
        luxnon_df.name = 'Non-Luxury / Luxury'

        redshift_sql6 = f"""select domimp as "Domestic/Import",COUNT(*) from auto_mmdb_infutor group by domimp order by domimp"""
        domimp_df = redshift_hook.get_pandas_df(redshift_sql6)
        domimp_df.loc[domimp_df["domestic/import"] == "D", "domestic/import"] = "Domestic"
        domimp_df.loc[domimp_df["domestic/import"] == "I", "domestic/import"] = "Import"
        domimp_df.name = 'Domestic / Import'

        redshift_sql7 = f"""select fuel_type as "Fuel Type",COUNT(*) from auto_mmdb_infutor group by fuel_type order by fuel_type"""
        fueltype_df = redshift_hook.get_pandas_df(redshift_sql7)
        fueltype_df.loc[fueltype_df["fuel type"]== "B", "fuel type"] = "B - Bio Diesel"
        fueltype_df.loc[fueltype_df["fuel type"]== "D", "fuel type"] = "D - Diesel"
        fueltype_df.loc[fueltype_df["fuel type"]== "F", "fuel type"] = "F - Flex Fuel"
        fueltype_df.loc[fueltype_df["fuel type"]== "G", "fuel type"] = "G - Gasoline"
        fueltype_df.loc[fueltype_df["fuel type"]== "H", "fuel type"] = "H - Hydrogen Fuel Cell"
        fueltype_df.loc[fueltype_df["fuel type"]== "I", "fuel type"] = "I - Plug-in Hybrid / Electric with Gas Generator"
        fueltype_df.loc[fueltype_df["fuel type"]== "L", "fuel type"] = "L - Electric"
        fueltype_df.loc[fueltype_df["fuel type"]== "N", "fuel type"] = "N - Natural Gas"
        fueltype_df.loc[fueltype_df["fuel type"]== "P", "fuel type"] = "P - Propane"
        fueltype_df.loc[fueltype_df["fuel type"]== "Y", "fuel type"] = "Y - Gas/Electric Hybrid"
        fueltype_df.name = 'Fuel type'
        
        redshift_sql8 = f"""select year as "Year of Auto",COUNT(*) from auto_mmdb_infutor group by year order by year"""
        year_df = redshift_hook.get_pandas_df(redshift_sql8)
        year_df.name = 'Year of Auto'

        redshift_sql9 = f"""select transyear as "Transaction Year",COUNT(*) from auto_mmdb_infutor group by transyear order by transyear"""
        transyear_df = redshift_hook.get_pandas_df(redshift_sql9)
        transyear_df.name = 'Transaction year'

        df_list = (make_df,bodytype_df,drivetype_df,vehtype_df,luxnon_df,domimp_df,fueltype_df,year_df,transyear_df)

        with io.BytesIO() as output:
            with pd.ExcelWriter(output) as writer:
                workbook = writer.book
                worksheet = workbook.add_worksheet('Result')
                writer.sheets['Result'] = worksheet

                COLUMN = 0
                row = 0

                for df in df_list:
                    worksheet.write_string(row, COLUMN, df.name)
                    row += 1
                    df.to_excel(writer, 
                                sheet_name='Result',
                                startrow=row, 
                                startcol=COLUMN,
                                index=False)
                    row += df.shape[0] + 4
            data = output.getvalue()
        s3 = boto3.resource('s3')
        key_name = 'Reports/DataAxle_Auto_MMDB_Profile_'+time_stamp+'_Infutor.xlsx'
        s3.Bucket('idms-7933-internalfiles').put_object(Key=key_name, Body=data)


    (cluster_resume()>>
        parallel_task1 >> 
        data_load4() >> 
        parallel_task2 >> 
        combined_child()>>
        data_load9() >> 
        cluster_pause() >>
        data_load10()>>
        write_excel()>>
        parallel_task4>>
        build_activate()>>
        send_notification()>>
        trigger_Fill_rate_QC_report
    )

Consumer_dag = pipeline()