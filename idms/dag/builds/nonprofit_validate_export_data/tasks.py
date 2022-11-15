import boto3, logging
from airflow.models import Variable
from helpers.sqlserver import get_sqlserver_hook
import awswrangler as wr
import pandas as pd
import time

# sql server connection
sqlhook = get_sqlserver_hook()
athena_temp_bucket = "s3://idms-7933-athena/"  # Variable.get("var-s3-bucket-names", deserialize_json=True).get("s3-athena-temp")
db_dictionary = {}
athena_database = ""
boto3.setup_default_session(region_name="us-east-1")
athena_task_max_retries = 3
default_sleep_time = 60 * 5


# pass db_dictionary,athena_database,DatabaseID,BuildID,OwerID,ExportTransaction
def get_runtime_args(**kwargs):
    global db_dictionary, athena_database, DatabaseID, BuildID, OwerID, ExportTransaction, RecencyDate

    if kwargs["dag_run"].conf.get("databaseid") == None:
        raise ValueError("Please pass DatabaseID")
    else:
        DatabaseID = kwargs["dag_run"].conf.get("databaseid")

    if kwargs["dag_run"].conf.get("buildid") == None:
        raise ValueError("Please pass BuildID")
    else:
        BuildID = kwargs["dag_run"].conf.get("buildid")

    if kwargs["dag_run"].conf.get("ownerid") == None:
        raise ValueError("Please pass ownerid")
    else:
        OwerID = kwargs["dag_run"].conf.get("ownerid")

    if kwargs["dag_run"].conf.get("exporttransaction") == None:
        raise ValueError("Please pass Export Transaction Flag Y or N")
    else:
        ExportTransaction = kwargs["dag_run"].conf.get("exporttransaction")

    if kwargs["dag_run"].conf.get("recencydate") == None and DatabaseID == 1438:
        RecencyDate = "2001-01-01"
    elif kwargs["dag_run"].conf.get("recencydate") == None and DatabaseID == 74:
        RecencyDate = "1950-01-01"
    else:
        RecencyDate = kwargs["dag_run"].conf.get("recencydate")

    db_dictionary = Variable.get(f"""var-db-{DatabaseID}""", deserialize_json=True)
    athena_database = "idmsprod"  # db_dictionary.get("athena_db_name")


def execute_athena_query(query, database, output_path):
    failures = 0
    while True:
        try:
            return wr.athena.read_sql_query(
                sql=query,
                database=database,
                s3_output=output_path,
            )
        except Exception as e:
            failures += 1
            if (failures == athena_task_max_retries):
                raise Exception("Exhausted maximum number of retries. Last error stacktrace as follows", e)
            time.sleep(default_sleep_time)


def get_list_of_ownerid(OwnerID):
    sql = f"""SELECT B.OwnerID as OwnerID, b.ID MasterLoLID, a.LK_Action
		  FROM DW_Admin.dbo.tblBuildLoL a (nolock)
		 INNER JOIN DW_Admin.dbo.tblMasterLoL b (nolock) on a.MasterLoLID = b.ID
         INNER JOIN DW_Admin.dbo.tblListLoadStatus s (nolock) 
            ON a.ID = s.BuildLoLID AND s.iIsCurrent = 1 AND s.LK_LoadStatus IN ('120','125')         
		 WHERE a.BuildID = {BuildID}
		   AND a.LK_Action IN ('R','N','A','D','O')
           AND b.OwnerID = {OwnerID}
           AND LK_ListType = 'C'
           -- Exclude FEC Lists for Apogee
           AND b.ID NOT IN (16972,16975,16977,16979,16981,16983,16985,16987,16989,16991,16993,16995,16997) 
		 ORDER BY B.OwnerID"""
    return sqlhook.get_pandas_df(sql)


def get_list_name():
    sql = f"""SELECT b.DatabaseID as databaseid,
                           b.OwnerID as listownerid,
                           b.ID as listid,
                           a.BuildID as buildID,
                           b.cListName as "list name",
                           Case WHEN a.LK_Action = 'R' THEN 'Replace'
					            WHEN a.LK_Action = 'N' THEN 'New'
					            WHEN a.LK_Action = 'A' THEN 'Add'
                                WHEN a.LK_Action = 'O' THEN 'Reuse' ELSE '' END as "list action"
		  FROM DW_Admin.dbo.tblBuildLoL a (nolock)
		 INNER JOIN DW_Admin.dbo.tblMasterLoL b (nolock) on a.MasterLoLID = b.ID
		 WHERE a.BuildID = {BuildID} and 
         b.DatabaseID = {DatabaseID} """
    return sqlhook.get_pandas_df(sql)


def get_transactionlistids(OwnerId):
    TransactionListIDs_sql = f"""SELECT CAST(a.ID as varchar) as ID
		                             FROM DW_Admin.dbo.tblMasterLoL a
		                            INNER JOIN DW_Admin.dbo.tblBuildLoL b
		                            ON a.ID = b.MasterLoLID AND b.LK_Action IN ('R','N','A','D','O') AND b.BuildID = {BuildID}
		                            WHERE OwnerID = {OwnerId}
		                            AND LK_ListType = 'E'"""
    TransactionListIDs_df = sqlhook.get_pandas_df(TransactionListIDs_sql)

    return TransactionListIDs_df


def fetch_last_inserted_id(DonorListID, redshift_hook):
    redshift_sql2 = f"""select max(id) as id from Exclude_ListConversion_Audit WHERE ListID = {DonorListID} AND BuildID = {BuildID}"""
    return redshift_hook.get_pandas_df(redshift_sql2)


def get_athena_donor_counts(DonorTableName):
    sql = f"""SELECT COUNT(*) as idonor, 
            COUNT(DISTINCT upper(accountno)) as iuniqueaccountdonor,
            SUM(CASE WHEN DeceasedFlag = 'Y' THEN 1 ELSE 0 END) as idonordeceased,
            SUM(CASE WHEN substr(MailabilityScore, 1,1) = '5' THEN 1 ELSE 0 END) as idonornonmailable
            FROM {DonorTableName}_build_output 
            WHERE (DropFlag IS NULL OR DropFlag = '')"""
    return execute_athena_query(sql, athena_database, athena_temp_bucket)


def export_athena_to_s3(
        TransactionSQL, DonorListID, output_bucket, output_bucket_prefix
):
    # Delete S3 folder if it was created after Summary Table’s dCreated Date.
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(output_bucket)
    bucket.objects.filter(Prefix=f"{output_bucket_prefix}{DonorListID}").delete()
    res = wr.athena.unload(
        sql=f"""SELECT ID,ListID,Individual_ID,Company_ID,AccountNo,
                ListCategory01,F1,ListCategory02,ListCategory03,ListCategory04,
                F4,F5,F6,F7,F8,F9,F10,F11,F12,F13,F14,Detail_DonationDollar,
                Detail_DonationDate,Detail_PaymentMethod,Detail_DonationChannel 
                from ({TransactionSQL})""",
        database=athena_database,
        file_format="TEXTFILE",
        field_delimiter="|",
        compression="gzip",
        path=f"s3://{output_bucket}/{output_bucket_prefix}{DonorListID}/",
    )


def export_athena_to_s3_with_retry(
        TransactionSQL, DonorListID, output_bucket, output_bucket_prefix
):
    failures = 0
    while True:
        try:
            export_athena_to_s3(TransactionSQL, DonorListID, output_bucket, output_bucket_prefix)
        except Exception as e:
            failures += 1
            if (failures == athena_task_max_retries):
                raise Exception("Exhausted maximum number of retries. Last error stacktrace as follows", e)
            time.sleep(default_sleep_time)


def check_s3_timestamp(
        etl_bucket,
        DonorTableName,
        output_bucket,
        DonorListID,
        output_bucket_prefix,
        etl_bucket_prefix,
        dcreateddate,
):
    # check created date for donor and transaction table and s3 folder date
    s3 = boto3.resource("s3")
    etl_bucket_obj = s3.Bucket(etl_bucket)
    etl_files = list(
        etl_bucket_obj.objects.filter(Prefix=f"{etl_bucket_prefix}{DonorTableName}")
    )
    etl_s3_object = s3.Object(etl_bucket, etl_files[0].key)
    etl_lastmodifieddate = etl_s3_object.last_modified

    output_bucket_obj = s3.Bucket(output_bucket)
    output_files = list(
        output_bucket_obj.objects.filter(Prefix=f"{output_bucket_prefix}{DonorListID}")
    )
    if len(output_files) > 0:
        output_s3_object = s3.Object(output_bucket, output_files[0].key)
        output_lastmodifieddate = output_s3_object.last_modified
        if ExportTransaction == "Y":
            if (
                    etl_lastmodifieddate > output_lastmodifieddate
            ):  # dcreatedate if exporttrasaction = N
                logging.info(
                    f"More Recent ETL Conversion Date {etl_lastmodifieddate} than exported Date {output_lastmodifieddate}"
                )
                return True
            else:
                return False
        elif ExportTransaction == "N":
            if pd.Timestamp(etl_lastmodifieddate).tz_localize(None) > pd.Timestamp(
                    dcreateddate
            ).tz_localize(
                None
            ):  # dcreatedate if exporttrasaction = N
                return True
            else:
                return False
    else:
        return True
