from datetime import datetime
from airflow.models import Variable
from airflow.decorators import dag, task
from helpers.sqlserver import get_build_df
from helpers.common import prepare_sql_file_using_df
import os, logging
import pandas as pd
from helpers.s3 import get_files_using_wr
from helpers.redshift import *
from airflow.operators.email_operator import EmailOperator


default_args = {
    "owner": "data-axle",
    "start_date": datetime(2022, 7, 25),
    "depends_on_past": False,
    'databaseid': 1489, #for prod dbid = 1489
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-aetna-failure")],
    "email_on_success": [Variable.get("var-email-on-aetna-success")],
    "email_on_failure": True,
    "email_on_retry": False,
}

@dag(
    default_args=default_args,
    dag_id="builds-aetna",
    description="Load Aetna Data into RedShift",
    schedule_interval=None,
    max_active_runs=10,
)

def pipeline():
    redshift_connection_var = "var-redshift-aetna-conn"
    redshift_iam_role_var = "var-password-redshift-iam-role-aetna"

    schema_file_path = default_args["currentpath"] + "/aetna_schema.csv"
    schema_df = pd.read_csv(schema_file_path,sep = '|')

    table_dict_path=default_args['currentpath'] + '/table_dict.json'
    with open(table_dict_path) as f:
        table_dict = json.load(f)


    env = Variable.get("var-env", "undefined")
    if env == "prod":
        s3_folder = '/mzb_input/'
    else:
        s3_folder = '/tg/'

    bdf = get_build_df(databaseid=default_args["databaseid"], active_flag=1)

    def get_runtime_args(**kwargs):
        global file_key,bucket_name,tabletype
        if kwargs['dag_run'].conf.get('file_key') == None:
            raise ValueError('Please pass filekey')
        else:
            file_key = kwargs['dag_run'].conf.get('file_key') #mzb_signal/ready_tablename.dat

        if kwargs['dag_run'].conf.get('bucket_name') == None:
            raise ValueError('Please pass bucket_name')
        else:
            bucket_name = kwargs['dag_run'].conf.get('bucket_name')#aetna_bucket

        if kwargs['dag_run'].conf.get('tabletype') == None:
            tabletype =''
        else:
            tabletype = kwargs['dag_run'].conf.get('tabletype')

    def set_table_variables(**kwargs):
        global temp_tablename,rstablename,tabletype, deletemonth,uniqueidentifier,create_table_names
        get_runtime_args(**kwargs)

        temp_tablename = file_key.split("/")[-1].split(".")[0][6:].upper() #tablename
        rstablename = temp_tablename 
        if tabletype == '':
            tabletype = table_dict[rstablename]["tabletype"]
        deletemonth = table_dict[rstablename]["deletemonth"]
        uniqueidentifier = table_dict[rstablename]["uniqueidentifier"]
            
        if tabletype == 'append':
            create_table_names = [f"{temp_tablename}_tmp"]
        elif tabletype == 'insert_delete':
            create_table_names = [f"{temp_tablename}_tmp",f"DELETE_{temp_tablename}"]
        else:
            create_table_names = [temp_tablename]

        if table_dict[temp_tablename]["tabletype"] in('append','insert_delete') and tabletype == 'full_refresh':
            create_table_names = [f"{temp_tablename}_tmp"]

    def send_email_without_config(**kwargs):
        dag = kwargs.get("errormessage")
        email_business_users = []
        email_business_users.append(Variable.get("var-email-on-aetna-success"))

        current_env = Variable.get("var-env", "Unknown")
        email_subject = ("Airflow " + current_env + " Notification:" + kwargs.get("email_subject"))
        email_message = kwargs.get("email_message")

        email = EmailOperator(
        mime_charset="utf-8",
        task_id="email_task",
        to=email_business_users,
        subject=email_subject,
        html_content=email_message,
        dag=dag,)

        email.execute(context=kwargs)
        
    #create table in redshift
    @task()
    def create_table(**kwargs):
        set_table_variables(**kwargs)
        for t in create_table_names:
            ddl = ""
            filter_df = schema_df[schema_df["file_type"] == t]
            logging.info(f"Creating {t}")

            ddl = get_create_table_ddl(t, filter_df ,default_args['databaseid'])
            executenonquery(ddl, connection = redshift_connection_var, iam = redshift_iam_role_var)

    # copy using manifest file
    @task()
    def copy_tables_using_manifest(**kwargs):  
        set_table_variables(**kwargs) 
        for t in create_table_names:
            logging.info(f"processing {t}")
            manifest_file = f"manifest/{t}_manifest.json"
            tname = t

            if tabletype in('append','insert_delete'):
                t= t.replace("_tmp",'')

            files = get_files_using_wr(f"s3://{bucket_name}{s3_folder}{t}_*.gz")
            generate_manifest_json(bucket_name, manifest_file, files)

            sql = prepare_sql_file_using_df(
                sql_file=f"{default_args['currentpath']}/010-copy-table.sql", fetch_buildinfo=bdf
            )

            sql = sql.replace("{table_name}", tname).replace(
                "{manifest_file}", f"s3://{bucket_name}/{manifest_file}"
            )
            if schema_df.loc[schema_df['file_type'] == tname, 'system_column'].iloc[0] == 'y':
                list_of_columns = list(schema_df.loc[schema_df['file_type'] == tname,'column_name'])
                list_of_columns.remove('id')
                list_of_columns.remove('listid')
                s = ','.join(list_of_columns)
                sql = sql.replace("{without_system_column}",f"({s})")
            else:
                sql = sql.replace("{without_system_column}",'')
                
            executenonquery(sql,connection = redshift_connection_var, iam = redshift_iam_role_var)

    def delete_month_data(rstablename,deletemonth,datecolumn):
        sql = f"""DELETE FROM {rstablename} 
                WHERE {datecolumn} <= (SELECT DATEADD(month,-{deletemonth},MAX({datecolumn})) FROM {rstablename});"""
        executenonquery(sql,connection = redshift_connection_var, iam = redshift_iam_role_var)


    @task()
    def load_append_data(**kwargs):
        set_table_variables(**kwargs)
        if tabletype == 'append':
            append_table(rstablename,f"{temp_tablename}_tmp",uniqueidentifier,redshift_connection_var,redshift_iam_role_var)
            delete_month_data(rstablename,deletemonth,'load_dt')
        elif tabletype == 'insert_delete':
            query = f"SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '{rstablename.lower()}')"
            query_result = executequery(query,connection = redshift_connection_var, iam = redshift_iam_role_var)
            if query_result[0][0]:
                pass
            else:
                create_empty_table(rstablename,f"{temp_tablename}_tmp",redshift_connection_var,redshift_iam_role_var)

            Delete_Command = f"DELETE FROM {rstablename} WHERE {uniqueidentifier} IN (SELECT DISTINCT {uniqueidentifier} FROM DELETE_{temp_tablename});"
            executenonquery(Delete_Command,connection = redshift_connection_var, iam = redshift_iam_role_var)
            if 'TBMZB_INDIVIDUAL_A' in rstablename:
                column_sql = f"""select column_name
                        from information_schema.columns
                        where table_name = LOWER('TBMZB_INDIVIDUAL_A_tmp')"""
                aetna_hook = getconnection(connection =redshift_connection_var)
                column_df = aetna_hook.get_pandas_df(column_sql)
                column_df = column_df.loc[column_df["column_name"] != 'id']
                columns = ','.join(column_df['column_name'].to_list())
                Insert_Command = f"INSERT INTO {rstablename} ( {columns} ) SELECT {columns} FROM {temp_tablename}_tmp;"
                executenonquery(Insert_Command,connection = redshift_connection_var, iam = redshift_iam_role_var)
            else:
                insert_into_table(rstablename,f"{temp_tablename}_tmp",redshift_connection_var,redshift_iam_role_var)
        else:
            pass

        if table_dict[temp_tablename]["tabletype"] in('append','insert_delete') and tabletype == 'full_refresh':
            sql = f"""DROP TABLE IF EXISTS {temp_tablename};
                    alter table {temp_tablename}_tmp RENAME TO {temp_tablename};"""
            executenonquery(sql,connection = redshift_connection_var, iam = redshift_iam_role_var)

    @task()
    def child_tables(bdf):
        sql = prepare_sql_file_using_df(
                sql_file=default_args["currentpath"] + "/05*.sql",
                fetch_buildinfo=bdf
                )      
        executenonquery(sql,connection = redshift_connection_var, iam = redshift_iam_role_var)


    @task()
    def send_notification():
        send_email_without_config(
        dag = dag,
        email_subject=  "aetna files processing",
        email_message= "Successfully process aetna files.<br/><br/>"
        )


    ( 
        create_table()
        >> copy_tables_using_manifest()
        >> load_append_data()
        >> child_tables(bdf)
        >> send_notification()
        
    )


aetna_dag = pipeline()
