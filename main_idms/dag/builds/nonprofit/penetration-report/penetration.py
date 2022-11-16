from datetime import date
from airflow.models import Variable
from airflow.decorators import dag, task
import os
from helpers.s3 import create_presigned_url
from helpers.nonprofit import get_donor_list_of_lists
from helpers.sqlserver import get_build_df
from helpers.nonprofit import get_config
from helpers.redshift import get_redshift_hook
from helpers.send_email import send_email_without_config
from helpers.common import remove_bad_chars
import pandas as pd
import boto3
import io
import airflow


default_args = {
    "owner": "data-axle",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": False,
    "currentpath": os.path.abspath(os.path.dirname(__file__)),
    "email": [Variable.get("var-email-on-failure-datalake")],
    "email_on_failure": False,
    "email_on_retry": False,
    "databaseid": 1438,
}

redshift_hook = get_redshift_hook()


@dag(
    default_args=default_args,
    dag_id="nonprofit-penetration-report",
    description="builds-nonprofit-penetration-report",
    schedule_interval="@once",
    max_active_runs=1,
)
def pipeline():
    def fetch_buildinfo():
        fetch_buildinfo_df = get_build_df(
            databaseid=default_args["databaseid"], active_flag=1
        )
        return fetch_buildinfo_df

    def get_runtime_args(fetch_build_df, **kwargs):
        global DatabaseID, BuildID, Build, Consumer_listid, NOM
        if kwargs["dag_run"].conf.get("databaseid") == None:
            DatabaseID = default_args["databaseid"]
        else:
            DatabaseID = kwargs["dag_run"].conf.get("databaseid")

        if kwargs["dag_run"].conf.get("buildid") == None:
            BuildID = fetch_build_df["build_id"]
        else:
            BuildID = kwargs["dag_run"].conf.get("buildid")

        if kwargs["dag_run"].conf.get("build") == None:
            Build = fetch_build_df["build"]
        else:
            Build = kwargs["dag_run"].conf.get("build")

        if kwargs["dag_run"].conf.get("consumer_listid") == None:
            Consumer_listid = get_config(DatabaseID, "consumer-listid")
        else:
            Consumer_listid = kwargs["dag_run"].conf.get("consumer-listid")

        if kwargs["dag_run"].conf.get("NOM") == None:
            NOM = 24
        else:
            NOM = kwargs["dag_run"].conf.get("nom")

    @task
    def generate_excel(fetch_build_df, **kwargs):
        get_runtime_args(fetch_build_df, **kwargs)
        env = Variable.get("var-env", "undefined")
        if env == "prod":
            bucket_name = Variable.get(
                "var-s3-bucket-names", deserialize_json=True
            ).get("s3-temp")
            df_lol = get_donor_list_of_lists(BuildID)
            temp_df = df_lol[["listid", "listname"]]

        else:
            bucket_name = "develop_idms-2722-playground"
            data = [
                [19626, "tom:my"],
                [19596, "nick"],
                [18912, "juli"],
                [19102, "harry"],
                [19520, "james"],
            ]
            temp_df = pd.DataFrame(data, columns=["sourcelistid", "listName"])

        Child_table_name = get_config(DatabaseID, "child_table_name")
        space = 10
        url_string = f"<table cellpadding={str(space)} cellspacing={str(space)}><tr><th>SOURCE_LIST_ID</th><th>LIST_NAME</th></tr>"

        temp_df["listName"] = temp_df["listName"].map(remove_bad_chars)
        for source_listid, listname in temp_df.itertuples(index=False):

            scriptfilename = default_args["currentpath"] + "/10-get-count.sql"
            filehandle = open(scriptfilename, "r")
            strScript = filehandle.read()
            filehandle.close()

            strScript = strScript.replace("{Source_List_ID}", str(source_listid))
            strScript = strScript.replace("{buildid}", str(BuildID))
            strScript = strScript.replace("{build}", str(Build))
            strScript = strScript.replace("{child_table_name}", Child_table_name)
            strScript = strScript.replace("{NOM}", str(NOM))

            df = redshift_hook.get_pandas_df(strScript)

            Total_Count = df.loc[0, "total_donors"]

            scriptfilename = default_args["currentpath"] + "/01-core-affinity.sql"
            filehandle = open(scriptfilename, "r")
            strScript = filehandle.read()
            filehandle.close()

            strScript = strScript.replace("{total_donors}", str(Total_Count))
            strScript = strScript.replace("{Source_List_ID}", str(source_listid))
            strScript = strScript.replace("{buildid}", str(BuildID))
            strScript = strScript.replace("{build}", str(Build))
            strScript = strScript.replace("{child_table_name}", Child_table_name)
            strScript = strScript.replace("{NOM}", str(NOM))
            strScript = strScript.replace("{consumer_listid}", str(Consumer_listid))

            df = redshift_hook.get_pandas_df(strScript)

            final_df = pd.merge(
                df, temp_df, left_on="sourcelistid", right_on="sourcelistid", how="left"
            )

            final_df = final_df.sort_values(by="overlap", ascending=False)
            final_df.reset_index(drop=True, inplace=True)

            with io.BytesIO() as output:
                with pd.ExcelWriter(output) as writer:

                    final_df.to_excel(writer, sheet_name="Result", index=False)

                data = output.getvalue()
            s3 = boto3.resource("s3")
            key = f"{str(DatabaseID)}-{str(BuildID)}-{str(source_listid)}-{str(listname)}-Penetration-Report.xlsx"
            s3.Bucket(bucket_name).put_object(Key=key, Body=data)
            url = create_presigned_url(bucket_name, key)
            if url is not None:
                html_string = f"<tr><td>{str(source_listid)}</td><td><a href={url}>{listname}</a></td></tr>"
                url_string = url_string + html_string

        url_string = url_string + "</table>"
        return url_string

    @task
    def send_report_email(fetch_build_df, report, **kwargs):
        Run_Date = date.today()
        get_runtime_args(fetch_build_df, **kwargs)
        send_email_without_config(
            dag=dag,
            email_subject=f"Airflow Notification:{str(DatabaseID)}-{str(BuildID)}-Penetration-Reports - {str(Run_Date)}",
            email_message=report,
            email_address="saakshi.dixit@celebaltech.com",
        )

    (send_report_email(fetch_buildinfo(), generate_excel(fetch_buildinfo())))


nonprofit_penetration_dag = pipeline()
