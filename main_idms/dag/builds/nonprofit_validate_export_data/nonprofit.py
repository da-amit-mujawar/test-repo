import logging
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from helpers.send_email import send_email_without_config
from airflow.hooks.postgres_hook import PostgresHook
import awswrangler as wr
import boto3
from builds.nonprofit_validate_export_data import tasks
import datetime
import pandas as pd
import time


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

dag = DAG(
    "nonprofit-validate-export-data",
    default_args=default_args,
    description="Nonprofit list conversion data validation and export",
    schedule_interval="@once",
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

# redshift connection
redshift_conn_id = Variable.get("var-redshift-da-rs-01-connection")
redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
athena_temp_bucket = "s3://develop_idms-7933-athena/"  # Variable.get("var-s3-bucket-names", deserialize_json=True).get("s3-athena-temp")
s3_internal = Variable.get("var-s3-bucket-names", deserialize_json=True)["s3-internal"]
boto3.setup_default_session(region_name="us-east-1")
DatabaseID = 0
BuildID = 0
entire_dag_max_retries = 2
default_sleep_time = 60 * 5


def folder_exists(bucket: str, path: str) -> bool:
    s3 = boto3.client("s3")
    path = path.rstrip("/")
    resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter="/", MaxKeys=1)
    return "CommonPrefixes" in resp


def export_data_with_retry(**kwargs):
    failures = 0
    while True:
        try:
            export_data(**kwargs)
        except Exception as e:
            failures += 1
            if (failures == entire_dag_max_retries):
                raise Exception("Exhausted maximum number of retries. Last error stacktrace as follows", e)
            time.sleep(default_sleep_time)


def export_data(**kwargs):
    # run time arguments
    tasks.get_runtime_args(**kwargs)
    if tasks.DatabaseID == 1438:
        output_bucket = (
            "axle-donorbase-silver-sources"  # tasks.db_dictionary.get("output-bucket")
        )
        etl_bucket = (
            "axle-donorbase-silver-sources"  # tasks.db_dictionary.get("etl-bucket")
        )
        output_bucket_prefix = (
            "exports/transactions/"  # tasks.db_dictionary.get("output-bucket-prefix")
        )
        etl_bucket_prefix = (
            "etl/build-output/"  # tasks.db_dictionary.get("etl-bucket-prefix")
        )
    elif tasks.DatabaseID == 74:
        output_bucket = "develop_idms-7933-apogee"  # tasks.db_dictionary.get("output-bucket")
        etl_bucket = "develop_idms-7933-prod"  # tasks.db_dictionary.get("etl-bucket")
        output_bucket_prefix = (
            "exports/transactions/"  # tasks.db_dictionary.get("output-bucket-prefix")
        )
        etl_bucket_prefix = (
            "etl/apogee/build-output/"  # tasks.db_dictionary.get("etl-bucket-prefix")
        )

    # Check if OwerID is null or not
    if tasks.OwerID > 0:
        b_OwerID = tasks.OwerID
    else:
        b_OwerID = "b.OwnerID"

    df_of_ownerid = tasks.get_list_of_ownerid(b_OwerID)
    sql = f"SELECT ListID, MAX(dCreatedDate) as dcreateddate FROM exclude_listconversion_audit WHERE BuildID = {tasks.BuildID} GROUP BY ListID"
    dCreatedDate_df = redshift_hook.get_pandas_df(sql)

    def parallel_process_ownerid(ownerid, donor_listid, donor_listaction):
        if ownerid != 0:
            DonorListID = donor_listid
            lk_action = donor_listaction
            is_s3_data_updated = False
            DonorTableName = (
                f"DW_Final_{tasks.DatabaseID}_{tasks.BuildID}_{DonorListID}"
            )
            logging.info(f"Processing {DonorTableName}")

            # delete from export if lk_action  = D and continue for next owner_id
            if lk_action == "D":
                if folder_exists(
                    output_bucket, output_bucket_prefix + str(DonorListID)
                ):
                    s3 = boto3.resource("s3")
                    bucket = s3.Bucket(output_bucket)
                    bucket.objects.filter(
                        Prefix=f"{output_bucket_prefix}{DonorListID}"
                    ).delete()
                redshift_sql4 = f"DELETE FROM Exclude_ListConversion_Audit WHERE listid = {DonorListID} AND BuildID = {tasks.BuildID};"
                redshift_hook.run(redshift_sql4, autocommit=True)
                logging.info(f"Deleted S3 Folder {DonorTableName} for Action=Remove")
            else:
                # check dw_final tables are updated or not
                # fetch dcreateddate for the same list and build top 1 if it is 0 then getdate()
                df_filtered = dCreatedDate_df[dCreatedDate_df["listid"] == DonorListID]
                if df_filtered.empty:
                    dcreateddate = datetime.datetime.now()
                    is_s3_data_updated = True
                else:
                    dcreateddate = df_filtered["dcreateddate"].iloc[0]

                if not is_s3_data_updated:
                    is_s3_data_updated = tasks.check_s3_timestamp(
                        etl_bucket,
                        DonorTableName,
                        output_bucket,
                        DonorListID,
                        output_bucket_prefix,
                        etl_bucket_prefix,
                        dcreateddate,
                    )

                if is_s3_data_updated:
                    logging.info(f"Athena Export Started {DonorTableName}")

                    TransactionListIDs_df = tasks.get_transactionlistids(ownerid)

                    # delete existing listid from audit table
                    redshift_sql1 = f"""DELETE FROM Exclude_ListConversion_Audit WHERE ListID = {DonorListID} AND BuildID = {tasks.BuildID};
                                    INSERT INTO Exclude_ListConversion_Audit (DatabaseID, BuildID, ListOwnerID, ListID, dCreatedDate) VALUES ({tasks.DatabaseID}, {tasks.BuildID}, {ownerid}, {DonorListID}, GETDATE());"""
                    redshift_hook.run(redshift_sql1, autocommit=True)

                    # Fetch ID of the last row inserted
                    ID = tasks.fetch_last_inserted_id(DonorListID, redshift_hook)

                    df_athena1 = tasks.get_athena_donor_counts(DonorTableName)
                    iDonor = df_athena1["idonor"].values[0]
                    iUniqueAccountDonor = df_athena1["iuniqueaccountdonor"].values[0]
                    iDonorDeceased = df_athena1["idonordeceased"].values[0]
                    iDonorNonMailable = df_athena1["idonornonmailable"].values[0]

                    TransactionSQL = ""
                    TransactionSQL1 = ""
                    for index, row in TransactionListIDs_df.iterrows():
                        if len(row["ID"]) > 1:
                            TransactionSQL = (
                                TransactionSQL
                                + f"""SELECT MAX(b.ID) ID, MAX(c.ListID) ListID, c.Individual_ID, MAX(c.Company_ID) Company_ID, replace(MAX(b.AccountNo),'|',' ') AccountNo, MAX(c.ListCategory01) ListCategory01, '0' F1, MAX(c.ListCategory02) ListCategory02, MAX(c.ListCategory03) ListCategory03, MAX(c.ListCategory04) ListCategory04,MAX(c.Zip) AS F4,'0' F5,' ' F6,' ' F7,' ' F8,' ' F9,'0' F10,'0' F11,'0' F12,'0' F13,' ' F14, b.Detail_DonationDollar, b.Detail_DonationDate, coalesce(MAX(b.Detail_PaymentMethod),'') Detail_PaymentMethod, coalesce(MAX(b.Detail_DonationChannel),'') Detail_DonationChannel,max(c.count_of_listcategory01) as count_of_listcategory01
                                                        FROM DW_Final_{tasks.DatabaseID}_{tasks.BuildID}_{row["ID"]}_Build_Output b 
                                                        INNER JOIN (SELECT upper(AccountNo) as AccountNo, Individual_ID, MAX(ListID) ListID, MAX(Company_ID) Company_ID, coalesce(MAX(ListCategory01),'') ListCategory01, coalesce(MAX(ListCategory02),'') ListCategory02, coalesce(MAX(ListCategory03),'') ListCategory03, coalesce(MAX(ListCategory04),'') ListCategory04, coalesce(MAX(Zip),'') Zip, COUNT(DISTINCT listcategory01) count_of_listcategory01 FROM {DonorTableName}_build_output
                                                        WHERE DeceasedFlag = 'N' AND substr(MailabilityScore, 1,1) <> '5' AND (DropFlag IS NULL OR DropFlag = '')
                                                        GROUP BY upper(AccountNo), Individual_ID) c 
                                                        ON upper(c.AccountNo) = upper(b.AccountNo)
                                                        WHERE cast(date_parse(b.Detail_DonationDate,'%Y%m%d') as timestamp) <= current_date  
                                                        AND b.Detail_DonationDate >='{tasks.RecencyDate}' AND (b.DropFlag IS NULL OR b.DropFlag = '')
                                                        GROUP BY c.Individual_ID, b.Detail_DonationDate, b.Detail_DonationDollar
                                                        UNION ALL """
                            )
                            TransactionSQL1 = (
                                TransactionSQL1
                                + f"""SELECT ID FROM DW_Final_{tasks.DatabaseID}_{tasks.BuildID}_{row["ID"]}_build_output WHERE (DropFlag IS NULL OR DropFlag = '') 
                                                            UNION ALL """
                            )

                    TransactionSQL = TransactionSQL[:-10]

                    if tasks.ExportTransaction == "Y":
                        tasks.export_athena_to_s3_with_retry(
                            TransactionSQL,
                            DonorListID,
                            output_bucket,
                            output_bucket_prefix,
                        )

                    sql3=f"""SELECT COUNT(*) AS itransaction FROM ({TransactionSQL1[:-10]}) a"""
                    df_athena3 = tasks.execute_athena_query(sql3, tasks.athena_database, athena_temp_bucket)

                    iTransaction = df_athena3["itransaction"].values[0]

                    sql4=f"""SELECT COUNT(*) AS ivalidtransaction,
                                                COUNT(DISTINCT Individual_ID) iuniqueindividualdonor,
                                                COUNT(DISTINCT Company_ID) iuniquehouseholddonor,
                                                SUM(CASE WHEN REGEXP_LIKE(cast(Individual_ID as varchar(25)), '^[0-9]+$') = true THEN 1 ELSE 0 END) iuniqueindividualmatch,
                                                SUM(CASE WHEN REGEXP_LIKE(cast(Company_ID as varchar(25)), '^[0-9]+$') = true THEN 1 ELSE 0 END) iuniquehouseholdmatch,
                                                AVG(CAST(Detail_DonationDollar as bigint)) iavgtransactiondollar, 
                                                cast(date_parse(MIN(Detail_DonationDate),'%Y%m%d') as date) dmintransactiondate,
                                                cast(date_parse(MAX(Detail_DonationDate),'%Y%m%d') as date) dmaxtransactiondate,
                                                MAX(ListCategory01) listcategory01, 
                                                MAX(ListCategory02) listcategory02,
                                                MAX(ListCategory03) listcategory03,
                                                MAX(ListCategory04) listcategory04,
                                                MAX(Count_of_listcategory01) count_of_listcategory01 
                                            FROM ({TransactionSQL}) a"""

                    df_athena4 = tasks.execute_athena_query(sql4, tasks.athena_database, athena_temp_bucket)

                    iValidTransaction = df_athena4["ivalidtransaction"].values[0]

                    # Do not continue if valid transaction is zero or null
                    if iValidTransaction == 0:
                        logging.info(f"No Valid Transactions Found {DonorTableName}")
                        return

                    iUniqueIndividualDonor = df_athena4[
                        "iuniqueindividualdonor"
                    ].values[0]
                    iUniqueHouseHoldDonor = df_athena4["iuniquehouseholddonor"].values[
                        0
                    ]
                    iUniqueIndividualMatch = df_athena4[
                        "iuniqueindividualmatch"
                    ].values[0]
                    iUniqueHouseholdMatch = df_athena4["iuniquehouseholdmatch"].values[
                        0
                    ]
                    iAvgTransactionDollar = df_athena4["iavgtransactiondollar"].values[
                        0
                    ]
                    dMinTransactionDate = df_athena4["dmintransactiondate"].values[0]
                    dMaxTransactionDate = df_athena4["dmaxtransactiondate"].values[0]
                    ListCategory01 = df_athena4["listcategory01"].values[0]
                    ListCategory02 = df_athena4["listcategory02"].values[0]
                    ListCategory03 = df_athena4["listcategory03"].values[0]
                    ListCategory04 = df_athena4["listcategory04"].values[0]
                    count_of_listcategory01 = df_athena4[
                        "count_of_listcategory01"
                    ].values[0]


                    sql5=f"""SELECT SUM(CASE WHEN regexp_like(cast(Individual_ID as varchar(25)), '^[0-9]+$') = true THEN 1 ELSE 0 END) iuniqueindividualmatchaftersuppress,
                                                                    SUM(CASE WHEN regexp_like(cast(Company_ID as varchar(25)), '^[0-9]+$') = true THEN 1 ELSE 0 END) iuniquehouseholdmatchaftersuppress
                                                                    FROM {DonorTableName}_build_output o
                                                                    WHERE DeceasedFlag = 'N'
                                                                    AND substr(MailabilityScore, 1,1) <> '5' AND (DropFlag IS NULL OR DropFlag = '')"""

                    df_athena5 = tasks.execute_athena_query(sql5, tasks.athena_database, athena_temp_bucket)

                    iUniqueIndividualMatchAfterSuppress = df_athena5[
                        "iuniqueindividualmatchaftersuppress"
                    ].values[0]
                    iUniqueHouseholdMatchAfterSuppress = df_athena5[
                        "iuniquehouseholdmatchaftersuppress"
                    ].values[0]

                    redshift_sql2 = f"""UPDATE Exclude_ListConversion_Audit
                                SET	iDonor = {iDonor}, 
                                    iUniqueAccountDonor = {iUniqueAccountDonor}, 
                                    iDonorDeceased = {iDonorDeceased}, 
                                    iDonorNonMailable = {iDonorNonMailable},
                                    iTransaction = {iTransaction},
                                    iValidTransaction = {iValidTransaction},
                                    iUniqueIndividualDonor = {iUniqueIndividualDonor},
                                    iUniqueHouseHoldDonor = {iUniqueHouseHoldDonor},
                                    iUniqueIndividualMatch = {iUniqueIndividualMatch},
                                    iUniqueHouseholdMatch = {iUniqueHouseholdMatch},
                                    iAvgTransactionDollar = {iAvgTransactionDollar},
                                    dMinTransactionDate = '{dMinTransactionDate}',
                                    dMaxTransactionDate = '{dMaxTransactionDate}',
                                    cListCategory01 = '{ListCategory01}',
                                    cListCategory02 = '{ListCategory02}',
                                    cListCategory03 = '{ListCategory03}',
                                    cListCategory04 = '{ListCategory04}',
                                    iUniqueIndividualMatchAfterSuppress = {iUniqueIndividualMatchAfterSuppress},
                                    iUniqueHouseholdMatchAfterSuppress = {iUniqueHouseholdMatchAfterSuppress},
                                    ilistcategory01count = {count_of_listcategory01}
                                WHERE ID = {ID["id"].values[0]}"""

                    redshift_hook.run(redshift_sql2, autocommit=True)

                    # -- Reset everything
                    DonorListID = ""
                    TransactionListIDs = ""
                else:
                    logging.info(f"Athena Export Skipped {DonorTableName}")

    # with futures.ThreadPoolExecutor() as executor:
    #     result = executor.map(parallel_process_ownerid, list_of_ownerid)
    for index, row in df_of_ownerid.iterrows():
        ownerid = row["OwnerID"]
        donor_listid = row["MasterLoLID"]
        donor_listaction = row["LK_Action"]

        parallel_process_ownerid(ownerid, donor_listid, donor_listaction)

    # Unload excel report to s3 location NonProfit_Validation_Report.csv by join redshift df with sqlserver df for
    # list name and list action
    df_listname = tasks.get_list_name()  # get listname and listaction
    redshift_df_sql = f"""SELECT DatabaseID as "Database ID",
                            ListOwnerID as "List Owner ID",
                            ListID as "List ID",
                            ilistcategory01count as "# of List Categories 01",
                            cListCategory01 as "List Category 01",
                            cListCategory02 as "List Category 02", 
                            cListCategory03 as "List Category 03",
                            cListCategory04 as "List Category 04",
                            iDonor as "Total donors",
                            iDonorDeceased as "Deceased Donors",
                            iDonorNonMailable as "NonMailable Donors",
                            iDonor - iDonorDeceased - iDonorNonMailable as "Valid Donors",
                            cast((iDonorNonMailable*1.0/iDonor) as decimal(10,6)) as "Mail Score Drop %",
                            iTransaction as "Total Transactions",
                            iValidTransaction as "Valid Transactions",
                            iUniqueIndividualDonor as "Unique Donors",
                            iUniqueHouseHoldDonor as "Unique HouseHolds",
                            iAvgTransactionDollar as "Avg Transactions Dollars",
                            dMinTransactionDate as "Min Transaction Date",
                            dMaxTransactionDate as "Max Transaction Date",
                            dCreatedDate as "Run Date"
                            FROM Exclude_ListConversion_Audit where buildid = {tasks.BuildID}"""
    redshift_df = redshift_hook.get_pandas_df(
        redshift_df_sql
    )  # get redshift df from audit table
    report_output_df = pd.merge(
        redshift_df,
        df_listname,
        left_on=["database id", "list owner id", "list id"],
        right_on=["databaseid", "listownerid", "listid"],
        how="left",
    )[
        [
            "database id",
            "list owner id",
            "list id",
            "list name",
            "list action",
            "# of list categories 01",
            "list category 01",
            "list category 02",
            "list category 03",
            "list category 04",
            "total donors",
            "deceased donors",
            "nonmailable donors",
            "valid donors",
            "mail score drop %",
            "total transactions",
            "valid transactions",
            "unique donors",
            "unique households",
            "avg transactions dollars",
            "min transaction date",
            "max transaction date",
            "run date",
        ]
    ]  # join df's
    # unload file to s3
    wr.s3.to_csv(
        df=report_output_df,
        path=f"s3://{s3_internal}/Reports/NonProfit_Validation_Report_{tasks.DatabaseID}_{tasks.BuildID}000",
        index=False,
    )


def send_email(**kwargs):
    # run time arguments
    tasks.get_runtime_args(**kwargs)
    send_email_without_config(
        email_business_users=["ApogeeUpdateTeam@data-axle.com","dl-donorbaseupdateteam@data-axle.com"],
        email_address="DWOrders@data-axle.com",
        reportname=f"/Reports/NonProfit_Validation_Report_{tasks.DatabaseID}_{tasks.BuildID}",
        dag=dag,
        email_subject=f"{tasks.DatabaseID}-{tasks.BuildID} List Validation Report",
        email_message="Please verify the attached list conversion report so we may proceed to the build process.",
    )


export_data = PythonOperator(
    task_id="export_data",
    python_callable=export_data_with_retry,
    provide_context=True,
    dag=dag,
)


# send success email
send_notification = PythonOperator(
    task_id="email_notification",
    python_callable=send_email,
    provide_context=True,
    dag=dag,
)
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> export_data >> send_notification >> end_operator
