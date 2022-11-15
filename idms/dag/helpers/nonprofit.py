from datetime import datetime
from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *
import pandas as pd
from pandas.io import sql as psql
from sqlalchemy import create_engine
import logging, boto3


def get_config(databaseid, item):
    env = Variable.get("var-env", "undefined")

    if env == "dev":
        config = {
            74: {
                "s3-silver-bucket": "idms-2722-apogee",
                "s3-silver-folder": "/exports/transactions/",
                "s3-etl-bucket": "idms-2722-apogee",
                "s3-etl-folder": "etl/build-output/",
                "consumer-listid": 21318,
                "s3-temp-bucket": "idms-2722-playground",
                "child_table_name": "tblChild3"
            },
            1438: {
                "s3-silver-bucket": "axle-donorbase-silver-sources",
                "s3-silver-folder": "/exports/transactions/",
                "s3-etl-bucket": "axle-donorbase-silver-sources",
                "s3-etl-folder": "etl/build-output/",
                "consumer-listid": 19946,
                "s3-temp-bucket": "idms-2722-playground",
                "child_table_name": "tblChild1"
            },
        }
    else:
        config = {
            74: {
                "s3-silver-bucket": "idms-7933-apogee",
                "s3-silver-folder": "/exports/transactions/",
                "s3-etl-bucket": "idms-7933-prod",
                "s3-etl-folder": "etl/apogee/build-output/",
                "consumer-listid": 21318,
                "s3-temp-bucket": "idms-7933-temp",
                "child_table_name": "tblChild3"
            },
            1438: {
                "s3-silver-bucket": "axle-donorbase-silver-sources",
                "s3-silver-folder": "/exports/transactions/",
                "s3-etl-bucket": "axle-donorbase-silver-sources",
                "s3-etl-folder": "etl/build-output/",
                "consumer-listid": 19946,
                "s3-temp-bucket": "idms-7933-temp",
                "child_table_name": "tblChild1"
            },
        }

    item = item.lower()
    return config[databaseid][item]


def get_donor_list_of_lists(buildid):
    sqlhook = get_sqlserver_hook()

    # Fetch only Donor Lists, excluding transactions and suppressions.
    sql = f"""
            SELECT MLoL.ID listid, MLoL.cListName listname, LK_Action action, iDropDuplicates dropunique, 
                   CAST(IIF(cCustomText6 = 'RETAIN-UNIQUE', 1, 0) as bit) retainunique
              FROM DW_Admin.dbo.tblBuildLoL BLoL (nolock) 
             INNER JOIN DW_Admin.dbo.tblMasterLoL MLoL (nolock) 
                ON BLoL.MasterLoLID=MLoL.ID     
             INNER JOIN DW_Admin.dbo.tblListLoadStatus s (nolock) 
                ON BLoL.ID = s.BuildLoLID AND s.iIsCurrent = 1 AND s.LK_LoadStatus IN ('120')     
             WHERE BLoL.buildid = {buildid}
               AND BLoL.LK_Action IN ('R', 'N', 'A', 'O')
               AND LK_ListType = 'C'
               AND MLoL.iIsActive = 1
               -- Exclude FEC Lists for Apogee
               AND MLoL.ID NOT IN (16972,16975,16977,16979,16981,16983,16985,16987,16989,16991,16993,16995,16997) 
             ORDER BY MLoL.ID
            """

    return sqlhook.get_pandas_df(sql)


def get_suppression_list_of_lists(buildid):
    sqlhook = get_sqlserver_hook()

    # Fetch Suppression Lists, excluding transactions, & donors.
    # Also donorbase doesn't support Adds/Incrementals & NoActions as full dataset has to go through matching and hygine every build
    sql = f"""
            SELECT MLoL.OwnerID, MLoL.ID listid, MLoL.cListName listname
              FROM DW_Admin.dbo.tblBuildLoL BLoL (nolock) 
             INNER JOIN DW_Admin.dbo.tblMasterLoL MLoL (nolock) 
                ON BLoL.MasterLoLID=MLoL.ID     
             INNER JOIN DW_Admin.dbo.tblListLoadStatus s (nolock) 
                ON BLoL.ID = s.BuildLoLID AND s.iIsCurrent = 1 AND s.LK_LoadStatus = '120'     
             WHERE BLoL.buildid = {buildid}
               AND BLoL.LK_Action IN ('R','N')
               AND LK_ListType = 'S'
               AND MLoL.iIsActive = 1
             ORDER BY MLoL.OwnerID, MLoL.ID
            """

    return sqlhook.get_pandas_df(sql)


def apogee_donortrans_count(buildid, **kwargs):

    allsql = """DROP TABLE IF EXISTS apogee_donortrans_count;
            CREATE TABLE apogee_donortrans_count(OwnerID varchar(4), type varchar(14), 
                tCount bigint, ListId int, ListAction varchar(1)); commit;
            INSERT INTO apogee_donortrans_count (OwnerID,Type,tCount,ListId,ListAction)
            """

    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = True

    with closing(redshift_conn.cursor()) as redshift_cursor:
        redshift_cursor.execute("select * from apogee_input_count")
        row = redshift_cursor.fetchone()
        while row:
            logging.info(row)
            ownerid, type, listid, listaction = row[0], row[1], row[3], row[4]
            if type == "TotalTrans":
                # MAXDATE
                insert_sql1 = f"""SELECT '{ownerid}' ,'MAX DATE' , CAST(CAST(MAX(DETAIL_DONATIONDATE) AS varchar(10)) AS int) , 
                            {listid}, '{listaction}' FROM dw_final_74_{buildid}_{listid}  WHERE nvl(DROPFLAG, '')='' UNION\n"""
                allsql = allsql + insert_sql1

                # MINDATE
                insert_sql2 = f"""SELECT '{ownerid}' ,'MIN DATE' , CAST(CAST(MIN(DETAIL_DONATIONDATE) AS varchar(10)) AS int) , 
                                       {listid}, '{listaction}' FROM dw_final_74_{buildid}_{listid}  WHERE nvl(DROPFLAG, '')=''
                                        AND nvl(DETAIL_DONATIONDATE, '')!='' AND nvl(DETAIL_DONATIONDATE, '')!='' UNION\n"""
                allsql = allsql + insert_sql2

                # TransConvert
                insert_sql3 = f"""SELECT '{ownerid}' ,'TransConvert' , count(*), {listid}, '{listaction}' 
                                            FROM dw_final_74_{buildid}_{listid}  WHERE nvl(DROPFLAG, '')='' UNION\n"""
                allsql = allsql + insert_sql3

            elif type == "Donor":
                # DonorConvert
                insert_sql4 = f"""SELECT '{ownerid}' ,'DonorConvert' , count(*),{listid}, '{listaction}' 
                            FROM dw_final_74_{buildid}_{listid}  WHERE nvl(DROPFLAG, '')='' UNION\n"""
                allsql = allsql + insert_sql4

            row = redshift_cursor.fetchone()

        allsql = allsql.strip()[:-5] + ";commit;"
        logging.info(allsql)
        redshift_cursor.execute(allsql)


def apogee_preaudit_report(buildid, databaseid, dag, *args, **kwargs):
    current_date = datetime.now()
    sqlhook = get_sqlserver_hook()

    # this is to replace the view ApogeePreUpdateAudit in sql server filtered by buildid
    # CB simplified query
    sql = f"""
            SELECT BLoL.buildid buildid, MLoL.cCode ccode, MLoL.ID listid, MLoL.cListName listname, LK_Action
              FROM DW_Admin.dbo.tblBuildLoL BLoL (nolock) INNER JOIN                                    
                   DW_Admin.dbo.tblMasterLoL MLoL (nolock) on BLoL.MasterLoLID=MLoL.ID     
             WHERE BLoL.buildid={buildid}
               AND BLoL.LK_Action IN ('R','N','A')
               AND MLoL.cCode NOT LIKE '%-S'
            """
    listname_df = sqlhook.get_pandas_df(sql)[["listid", "listname"]]
    logging.info(listname_df)

    redshift_hook = get_redshift_hook()
    count_df = redshift_hook.get_pandas_df(
        "select * from apogee_input_count union select * from apogee_donortrans_count"
    )
    count_pivot = (
        count_df.pivot_table(
            index=["ownerid", "listaction"],
            columns=["type"],
            values="tcount",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    logging.info(count_pivot)

    count_donor_df = redshift_hook.get_pandas_df(
        "select ownerid, listid as donor_listid from apogee_input_count where Type='Donor'"
    )
    count_trans_df = redshift_hook.get_pandas_df(
        "select ownerid, listid as trans_listid from apogee_input_count where Type='TotalTrans'"
    )

    count_both_df = count_donor_df.set_index("ownerid").join(
        count_trans_df.set_index("ownerid")
    )
    logging.info(count_both_df)

    count_list_df = count_pivot.merge(count_both_df, on="ownerid", how="left")
    count_final_df = count_list_df.merge(
        listname_df, left_on="donor_listid", right_on="listid", how="left"
    )

    df_report = count_final_df[
        [
            "ownerid",
            "listname",
            "donor_listid",
            "trans_listid",
            "listaction",
            "MIN DATE",
            "MAX DATE",
            "Donor",
            "DonorConvert",
            "TotalTrans",
            "TransConvert",
            "Distinct Donor",
            "CompanyIDs",
            "MatchingIDs",
        ]
    ]

    df_report["donor_to_distinct_donor"] = round(
        100
        * (df_report["Donor"] - df_report["Distinct Donor"])
        / df_report["Distinct Donor"],
        3,
    )
    df_report["donor_to_distinct_donor"] = (
        df_report["donor_to_distinct_donor"].astype(str) + "%"
    )

    # NOTE TO SF FROM CB: Change column headers here instead of above?
    df_report.columns = [
        "Owner ID",
        "List Name",
        "Donor ListID",
        "Transaction ListID",
        "List Action",
        "Min Date",
        "Max Date",
        "Donor Received Qty",
        "Donor Converted Qty",
        "Transaction Received Qty",
        "Transaction Converted Qty",
        "Distinct Donor",
        "CompanyIDs",
        "MatchingAccNos",
        "Donor to Distinct Donor",
    ]
    audit_report_name = f"/tmp/Pre-Update-Audit-for-BuildID-{buildid}-{current_date.strftime('%Y')}{current_date.strftime('%m')}{current_date.strftime('%d')}.xlsx"
    df_report.to_excel(audit_report_name, index=False)

    # write the final report to redshift table
    redshift_hook = get_redshift_hook()
    conn = redshift_hook.get_sqlalchemy_engine()
    df_report.to_sql("apogee_preaudit_report", conn, index=False, if_exists="replace")

    build_info = Variable.get(
        "var-db-" + str(databaseid), deserialize_json=True, default_var=None
    )
    build_info["pre_audit_report"] = audit_report_name
    Variable.set(f"var-db-{str(databaseid)}", json.dumps(build_info, indent=4))
    logging.info(build_info)
    return [audit_report_name]


def update_L2_matchcode(buildid, databaseid, *args, **kwargs):

    """
    This function is for updating the final tables with individual_id and company_id from the L2 reference tables
    if individual_id and/or company_id are empty
    """
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = True

    with closing(redshift_conn.cursor()) as redshift_cursor:
        redshift_cursor.execute(
            'select * from apogee_preaudit_report order by ["donor listid"]'
        )
        row = redshift_cursor.fetchone()
        while row:
            logging.info(row)
            donor_listid = row[2]
            sql_update_company_mc = f"""
                update dw_final_{databaseid}_{buildid}_{donor_listid}  set company_id=ref.company_id
                from l2_company_id_ref_table ref
                where dw_final_{databaseid}_{buildid}_{donor_listid}.company_mc=ref.company_mc and nvl(dw_final_{databaseid}_{buildid}_{donor_listid}.company_id, '')=''
            
            """
            sql_update_individual_mc = f"""
                update dw_final_{databaseid}_{buildid}_{donor_listid}  set individual_id=ref.individual_id
                from l2_individual_id_ref_table ref
                where dw_final_{databaseid}_{buildid}_{donor_listid}.individual_mc=ref.individual_mc and nvl(dw_final_{databaseid}_{buildid}_{donor_listid}.individual_id, '')=''
            """
            redshift_hook.execute(sql_update_company_mc)
            redshift_hook.execute(sql_update_individual_mc)

            row = redshift_cursor.fetchone()


def load_np_transactions(buildid, databaseid, *args, **kwargs):
    # load data into NP_Transactions - child3
    # skip the _bad.txt, _A.txt and combined.txt
    # load the rest of the final tables
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = True

    table_name = "apogee_transactions_np"

    # create temp child3 table
    sql = f"""DROP TABLE IF EXISTS {table_name};
                    CREATE TABLE {table_name} (
                        ID bigint identity (1,1),
                        TableID int,
                        ListID int, 
                        Individual_ID bigint,
                        Company_ID    bigint,
                        AccountNo varchar(50), 
                        ListCategory01 char(2), 
                        ListCategory02 char(2), 
                        ListCategory03 char(2), 
                        ListCategory04 char(2), 
                        ListCategory05 char(2), 
                        List_TotalNumberDonations int,                    
                        List_TotalDollarDonations int, 
                        List_LastDateDonation char(8), 
                        List_FirstDateDonation char(8), 
                        List_LastPaymentMethod char(1), 
                        List_LastChannel char(1), 
                        List_LastDollarDonation int, 
                        List_HighestDollarDonation int, 
                        List_LowestDollarDonation int, 
                        List_WeeksSinceLastDonation int, 
                        List_VolunteerInd varchar(1), 
                        Detail_DonationDollar int, 
                        Detail_DonationDate char(8), 
                        Detail_PaymentMethod char(1),
                        Detail_DonationChannel char(1)
                    ); 
    """
    redshift_hook.run(sql)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        redshift_cursor.execute(
            f"""select * from apogee_preaudit_report
        	where ["donor listid"]  in (select LISTID from apogee_list_values where cValue = 0 ) 
        	order by ["donor listid"]
        """
        )
        row = redshift_cursor.fetchone()
        while row:
            logging.info(row)
            # @BuildID, @DatabaseID, @OwnerID , @ListID , @Code, @lcAction
            ownerid, listid, trans_listid, action = row[0], row[2], row[3], row[4]

            # create deduped table for consumer and transaction
            sql_dedupe = f"""DROP TABLE IF EXISTS apogee_cust_dedupe;
                    CREATE TABLE apogee_cust_dedupe
                    (ID                          BIGINT NOT NULL, 
                     ListID                      INT NULL, 
                     Individual_ID               BIGINT NULL, 
                     Company_ID                  BIGINT NOT NULL, 
                     AccountNo                   VARCHAR(25) NOT NULL, 
                     ListCategory01              CHAR(2) NULL, 
                     List_TotalNumberDonations   INT NULL, 
                     ListCategory02              CHAR(2) NULL, 
                     ListCategory03              CHAR(2) NULL, 
                     ListCategory04              CHAR(2) NULL, 
                     ListCategory05              CHAR(2) NULL, 
                     List_TotalDollarDonations   INT NULL, 
                     List_LastDateDonation       CHAR(8) NULL, 
                     List_FirstDateDonation      CHAR(8) NULL, 
                     List_LastPaymentMethod      CHAR(1) NULL, 
                     List_LastChannel            CHAR(1) NULL, 
                     List_LastDollarDonation     INT NULL, 
                     List_HighestDollarDonation  INT NULL, 
                     List_LowestDollarDonation   INT NULL, 
                     List_WeeksSinceLastDonation INT NULL, 
                     List_VolunteerInd           CHAR(1) NULL   
                    );
                    INSERT INTO apogee_cust_dedupe
                    SELECT MAX(ID),
                    ListID, 
                    CASE WHEN (individual_id is null or individual_id = '') THEN 0 ELSE CAST(individual_id  as bigint) END, 
                    CAST(COMPANY_ID as BIGINT), 
                    LEFT(AccountNo,25) AccountNo, 
                    ListCategory01, 
                    List_TotalNumberDonations, 
                    ListCategory02, 
                    ListCategory03, 
                    ListCategory04, 
                    ListCategory05, 
                    List_TotalDollarDonations, 
                    List_LastDateDonation, 
                    List_FirstDateDonation, 
                    List_LastPaymentMethod, 
                    List_LastChannel, 
                    List_LastDollarDonation, 
                    List_HighestDollarDonation, 
                    List_LowestDollarDonation, 
                    List_WeeksSinceLastDonation, 
                    List_VolunteerInd
                FROM	dw_final_{databaseid}_{buildid}_{listid} 
                WHERE	nvl(company_id, '')!='' 
                  AND   nvl(AccountNo, '')!='' 
                  AND   nvl(DROPFLAG, '') =''
                GROUP BY ListID, 
                         individual_id, 
                         company_id, 
                         AccountNo, 
                         ListCategory01, 
                         List_TotalNumberDonations, 
                         ListCategory02, 
                         ListCategory03, 
                         ListCategory04, 
                         ListCategory05, 
                         List_TotalDollarDonations, 
                         List_LastDateDonation, 
                         List_FirstDateDonation, 
                         List_LastPaymentMethod, 
                         List_LastChannel, 
                         List_LastDollarDonation, 
                         List_HighestDollarDonation, 
                         List_LowestDollarDonation, 
                         List_WeeksSinceLastDonation, 
                         List_VolunteerInd;
                
                DROP TABLE IF EXISTS apogee_trans_dedupe;
                CREATE TABLE apogee_trans_dedupe
                (ID                          BIGINT NOT NULL, 
                 ListID                      INT NULL, 
                 AccountNo                   VARCHAR(25) NOT NULL, 
                 Detail_DonationDollar       INT NULL, 
                 Detail_DonationDate         CHAR(8) NULL, 
                 Detail_PaymentMethod        CHAR(1) NULL,
                 Detail_DonationChannel      CHAR(1) NULL
                );

                INSERT INTO apogee_trans_dedupe
                    SELECT MAX(ID),
                        ListID, 
                        LEFT(AccountNo,25) AccountNo, 
                        Detail_DonationDollar, 
                        Detail_DonationDate, 
                        Detail_PaymentMethod, 
                        Detail_DonationChannel
                    FROM	dw_final_{databaseid}_{buildid}_{trans_listid} 
                    WHERE	nvl(DROPFLAG, '') = ''
                      AND   nvl(AccountNo, '')!='' 
                    GROUP BY ListID, 
                             AccountNo, 
                             Detail_DonationDollar, 
                             Detail_DonationDate, 
                             Detail_PaymentMethod, 
                             Detail_DonationChannel;
            """
            ##dedupe again by ListID,  AccountNo, Detail_DonationDate,Detail_DonationDollar
            ## insert into child3

            redshift_hook.run(sql_dedupe)

            # join consumer with transaction to get all fields needed and dedupe again
            sql = """DROP TABLE IF EXISTS apogee_dedupe_stage;
                CREATE TABLE apogee_dedupe_stage
                (ID                          BIGINT IDENTITY(1,1),
                 ListID                      INT NULL, 
                 Individual_ID               BIGINT NULL, 
                 Company_ID                  BIGINT NOT NULL, 
                 AccountNo                   VARCHAR(25) NOT NULL, 
                 ListCategory01              CHAR(2) NULL, 
                 List_TotalNumberDonations   INT NULL, 
                 ListCategory02              CHAR(2) NULL, 
                 ListCategory03              CHAR(2) NULL, 
                 ListCategory04              CHAR(2) NULL, 
                 ListCategory05              CHAR(2) NULL, 
                 List_TotalDollarDonations   INT NULL, 
                 List_LastDateDonation       CHAR(8) NULL, 
                 List_FirstDateDonation      CHAR(8) NULL, 
                 List_LastPaymentMethod      CHAR(1) NULL, 
                 List_LastChannel            CHAR(1) NULL, 
                 List_LastDollarDonation     INT NULL, 
                 List_HighestDollarDonation  INT NULL, 
                 List_LowestDollarDonation   INT NULL, 
                 List_WeeksSinceLastDonation INT NULL, 
                 List_VolunteerInd           CHAR(1) NULL,
                 Detail_DonationDollar       INT NULL, 
                 Detail_DonationDate         CHAR(8) NULL, 
                 Detail_PaymentMethod        CHAR(1) NULL,
                 Detail_DonationChannel      CHAR(1) NULL
                );

            INSERT INTO apogee_dedupe_stage
                (ListID, individual_id, company_id, AccountNo, ListCategory01, List_TotalNumberDonations, ListCategory02, ListCategory03, ListCategory04, ListCategory05, 
                List_TotalDollarDonations, List_LastDateDonation, List_FirstDateDonation, List_LastPaymentMethod, List_LastChannel, List_LastDollarDonation, 
                List_HighestDollarDonation, List_LowestDollarDonation, List_WeeksSinceLastDonation, List_VolunteerInd, Detail_DonationDollar, Detail_DonationDate, 
                Detail_PaymentMethod, Detail_DonationChannel)
                SELECT  
                    a.ListID, 
                    a.individual_id, 
                    a.company_id, 
                    a.AccountNo, 
                    a.ListCategory01, 
                    a.List_TotalNumberDonations, 
                    a.ListCategory02, 
                    a.ListCategory03, 
                    a.ListCategory04, 
                    a.ListCategory05, 
                    a.List_TotalDollarDonations, 
                    a.List_LastDateDonation, 
                    a.List_FirstDateDonation, 
                    a.List_LastPaymentMethod, 
                    a.List_LastChannel, 
                    a.List_LastDollarDonation, 
                    a.List_HighestDollarDonation, 
                    a.List_LowestDollarDonation, 
                    a.List_WeeksSinceLastDonation, 
                    a.List_VolunteerInd,
                    b.Detail_DonationDollar, 
                    b.Detail_DonationDate, 
                    b.Detail_PaymentMethod,
                    b.Detail_DonationChannel
                FROM	apogee_cust_dedupe a
                JOIN	apogee_trans_dedupe b on a.AccountNo = b.AccountNo;
           
               DELETE FROM apogee_dedupe_stage
                    WHERE ID NOT IN
                    (
                    SELECT MAX(ID)
                    FROM apogee_dedupe_stage
                    GROUP BY ListID, 
                            AccountNo, 
                            Detail_DonationDate,
                            Detail_DonationDollar
                    );
     
            """
            redshift_hook.run(sql)

            # insert into child3 temp table
            sql = f"""
                INSERT INTO {table_name}
                (TableID, ListID, individual_id, company_id, AccountNo, ListCategory01, ListCategory02, ListCategory03, ListCategory04, ListCategory05, List_TotalNumberDonations,
                List_TotalDollarDonations, List_LastDateDonation, List_FirstDateDonation, List_LastPaymentMethod, List_LastChannel, List_LastDollarDonation, 
                List_HighestDollarDonation, List_LowestDollarDonation, List_WeeksSinceLastDonation, List_VolunteerInd, Detail_DonationDollar, Detail_DonationDate, 
                Detail_PaymentMethod, Detail_DonationChannel)
                SELECT
                    ID,
                    ListID, 
                    individual_id, 
                    company_id, 
                    AccountNo, 
                    ListCategory01, 
                    ListCategory02, 
                    ListCategory03, 
                    ListCategory04, 
                    ListCategory05, 
                    List_TotalNumberDonations, 
                    List_TotalDollarDonations, 
                    List_LastDateDonation, 
                    List_FirstDateDonation, 
                    List_LastPaymentMethod, 
                    List_LastChannel, 
                    List_LastDollarDonation, 
                    List_HighestDollarDonation, 
                    List_LowestDollarDonation, 
                    List_WeeksSinceLastDonation, 
                    List_VolunteerInd,
                    Detail_DonationDollar, 
                    Detail_DonationDate, 
                    Detail_PaymentMethod,
                    Detail_DonationChannel
                from apogee_dedupe_stage
            """

            if action == "A":
                sql2 = f"""  
                    select TableID, sourceListID, individual_id, company_id, AccountNo, ListCategory01, ListCategory02, ListCategory03, ListCategory04, ListCategory05, List_TotalNumberDonations,
                    List_TotalDollarDonations, List_LastDateDonation, List_FirstDateDonation, List_LastPaymentMethod, List_LastChannel, List_LastDollarDonation,
                    List_HighestDollarDonation, List_LowestDollarDonation, List_WeeksSinceLastDonation, List_VolunteerInd, Detail_DonationDollar, Detail_DonationDate,
                    Detail_PaymentMethod, Detail_DonationChannel
                    from apogee_iq_tblchild3 where sourcelistid = {listid}
                """

                sql = sql + " union " + sql2

            redshift_hook.run(sql)
            logging.info(f"Loading list {listid}")
            row = redshift_cursor.fetchone()
        logging.info("loading non-profit transactions is complete")


def update_category(buildid, databaseid, *args, **kwargs):
    sql_hook = get_sqlserver_hook()
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = True

    ## create the ListCategory table
    sql = f"""SELECT ID AS ListID,cCustomText9 FROM tblmasterLOL 
                    WHERE databaseID = {databaseid}
                    AND iIsActive = 1
                    AND cCustomText9 like ('cat%')
                    AND cCustomText9 like ('%03%')
           
            """

    cat_df = sql_hook.get_pandas_df(sql)
    conn = redshift_hook.get_sqlalchemy_engine()
    cat_df.to_sql("apogee_category_temp", conn, index=False, if_exists="replace")

    # UPDATE NP_Transactions SET ListCategory01='LI', ListCategory02='SS', ListCategory03='GC' WHERE LISTID =  7391 ;COMMIT;

    sql = f"""
            DROP TABLE IF EXISTS apogee_list_category_update;
            CREATE TABLE apogee_list_category_update
            (ListID INT,ListCategoryFull VARCHAR(100),ListCategory01 VARCHAR(50),ListCategory02 VARCHAR(50),
            LISTCATEGORY03 varchar(50));
            
            INSERT INTO apogee_list_category_update
            (ListID ,ListCategoryFull )
            SELECT ListID, cCustomText9 FROM apogee_category_temp;
            
            UPDATE apogee_list_category_update
            SET ListCategory01 = LEFT(ListCategoryFull,CHARINDEX(',',ListCategoryFull)-1),
                ListCategory02 = SUBSTRING(ListCategoryFull,CHARINDEX(',',ListCategoryFull)+1,LEN(ListCategoryFull));
            
            UPDATE apogee_list_category_update
            SET ListCategory03 = SUBSTRING(ListCategory02,CHARINDEX(',',ListCategory02)+5,LEN(ListCategory02));COMMIT;
            
            UPDATE apogee_list_category_update
            SET ListCategory02 = LTRIM(REPLACE(REPLACE(LEFT(ListCategory02,CHARINDEX(',',ListCategory02)), '02=', ''), ',', ''));

            UPDATE apogee_list_category_update
            SET ListCategory01 = REPLACE(LISTCATEGORY01,'CAT CODE 01=','');
                                                 
            """

    redshift_hook.run(sql)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        redshift_cursor.execute("select * from apogee_list_category_update")
        row = redshift_cursor.fetchone()
        while row:
            listid, category01, category02, category03 = row[0], row[2], row[3], row[4]
            update_sql = f"""
                    UPDATE NP_Transactions 
                    SET ListCategory01='{category01}', 
                        ListCategory02='{category02}', 
                        ListCategory03='{category03}'
                        WHERE LISTID = {listid};
            
            """
            redshift_hook.run(update_sql)
            logging.info(f"Update Category for list: {listid}")
            row = redshift_cursor.fetchone()
    logging.info("Non-profit transactions list category update is complete")


def load_fec_transactions(buildid, databaseid, *args, **kwargs):
    # load data into NP_Transactions - child3
    # skip the _bad.txt, _A.txt and combined.txt
    # load the rest of the final tables
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = True

    # create temp child3 table
    sql = """DROP TABLE IF EXISTS apogee_transactions_fec;
                    CREATE TABLE apogee_transactions_fec (
                        ID bigint identity (1,1),
                        TableID int,
                        ListID int, 
                        Individual_ID bigint,
                        Company_ID    bigint,
                        AccountNo varchar(100), 
                        ListCategory01 char(2), 
                        ListCategory02 char(2), 
                        ListCategory03 char(2), 
                        ListCategory04 char(2), 
                        ListCategory05 char(2), 
                        Party_Name varchar(20),
                        IsBlueFlag varchar(20),
                        CommitteeID varchar(20),
                        Raw_Field04 varchar(10),
                        Raw_Field05 varchar(10),
                        Raw_Field06 varchar(10),
                        Raw_Field07 varchar(10),
                        Raw_Field08 varchar(10),
                        Raw_Field09 varchar(10),
                        Raw_Field10 varchar(10),
                        Detail_DonationDollar int, 
                        Detail_DonationDate char(8), 
                        Detail_PaymentMethod char(1),
                        Detail_DonationChannel char(1)
                    ); 
    """
    redshift_hook.run(sql)

    with closing(redshift_conn.cursor()) as redshift_cursor:
        redshift_cursor.execute(
            """select * from apogee_preaudit_report
        	where ["donor listid"]  in (select LISTID from apogee_list_values where cValue > 0 ) 
        	order by ["donor listid"]
        """
        )
        row = redshift_cursor.fetchone()
        while row:

            # @BuildID, @DatabaseID, @OwnerID , @ListID , @Code, @lcAction
            ownerid, listid, trans_listid, action = row[0], row[2], row[3], row[4]

            # create deduped table for consumer and transaction
            sql_dedupe = f"""DROP TABLE IF EXISTS apogee_cust_dedupe_fec;
                    CREATE TABLE apogee_cust_dedupe_fec
                    (ID                          BIGINT NOT NULL, 
                     ListID                      INT NULL, 
                     Individual_ID               BIGINT NULL, 
                     Company_ID                  BIGINT NOT NULL, 
                     AccountNo                   VARCHAR(25) NOT NULL, 
                     ListCategory01              CHAR(2) NULL, 
                     List_TotalNumberDonations   INT NULL, 
                     ListCategory02              CHAR(2) NULL, 
                     ListCategory03              CHAR(2) NULL, 
                     ListCategory04              CHAR(2) NULL, 
                     ListCategory05              CHAR(2) NULL
                  
                    );
                    INSERT INTO apogee_cust_dedupe_fec
                    SELECT MAX(ID),
                    ListID, 
                    CASE WHEN (individual_id is null or individual_id = '') THEN 0 ELSE CAST(individual_id  as bigint) END, 
                    CAST(COMPANY_ID as BIGINT), 
                    LEFT(AccountNo,25) AccountNo, 
                    ListCategory01, 
                    List_TotalNumberDonations, 
                    ListCategory02, 
                    ListCategory03, 
                    ListCategory04, 
                    ListCategory05
                    
                FROM	dw_final_{databaseid}_{buildid}_{listid} 
                WHERE	nvl(company_id, '')!=''
                  AND   nvl(AccountNo, '')!=''  
                  AND   nvl(DROPFLAG, '') =''
                GROUP BY ListID, 
                         individual_id, 
                         company_id, 
                         AccountNo, 
                         ListCategory01, 
                         List_TotalNumberDonations, 
                         ListCategory02, 
                         ListCategory03, 
                         ListCategory04, 
                         ListCategory05;
                         

                DROP TABLE IF EXISTS apogee_trans_dedupe_fec;
                CREATE TABLE apogee_trans_dedupe_fec
                (ID                          BIGINT NOT NULL, 
                 ListID                      INT NULL, 
                 AccountNo                   VARCHAR(25) NOT NULL, 
                 Party     	  varchar(20) NULL,
                 IsBlue         varchar(20) NULL,
                 Committee_Id   varchar(20) NULL,
                 RAW_Field04    varchar(10) NULL,
                 RAW_Field05    varchar(10) NULL,
                 RAW_Field06    varchar(10) NULL,
                 RAW_Field07    varchar(10) NULL,
                 RAW_Field08    varchar(10) NULL,
                 RAW_Field09    varchar(10) NULL,
                 RAW_Field10    varchar(10) NULL,
                 Detail_DonationDollar       INT NULL, 
                 Detail_DonationDate         CHAR(8) NULL, 
                 Detail_PaymentMethod        CHAR(1) NULL,
                 Detail_DonationChannel      CHAR(1) NULL
                );

                INSERT INTO apogee_trans_dedupe_fec
                    SELECT MAX(ID),
                        ListID, 
                        LEFT(AccountNo,25) AccountNo, 
                        Detail_DonationDollar, 
                        Detail_DonationDate, 
                        Detail_PaymentMethod, 
                        Detail_DonationChannel
                    FROM	dw_final_{databaseid}_{buildid}_{trans_listid} 
                    WHERE	nvl(DROPFLAG, '') = ''
                      AND   nvl(AccountNo, '')!='' 
                    GROUP BY ListID, 
                             AccountNo, 
                             Detail_DonationDollar, 
                             Detail_DonationDate, 
                             Detail_PaymentMethod, 
                             Detail_DonationChannel;
            """
            ##dedupe again by ListID,  AccountNo, Detail_DonationDate,Detail_DonationDollar
            ## insert into child3

            redshift_hook.run(sql_dedupe)

            # join consumer with transaction to get all fields needed and dedupe again
            sql = """DROP TABLE IF EXISTS apogee_dedupe_stage;
            CREATE TABLE apogee_dedupe_stage
            (ID                          bigint IDENTITY(1,1),
             ListID                      INT NULL, 
             Individual_ID               BIGINT NULL, 
             Company_ID                  BIGINT NOT NULL, 
             AccountNo                   VARCHAR(25) NOT NULL, 
             ListCategory01              CHAR(2) NULL, 
             ListCategory02              CHAR(2) NULL, 
             ListCategory03              CHAR(2) NULL, 
             ListCategory04              CHAR(2) NULL, 
             ListCategory05              CHAR(2) NULL, 
             Party_Name             varchar(20),
             IsBlueFlag             varchar(20),
             CommitteeID                varchar(20),
             Raw_Field04 varchar(10),
             Raw_Field05 varchar(10),
             Raw_Field06 varchar(10),
             Raw_Field07 varchar(10),
             Raw_Field08 varchar(10),
             Raw_Field09 varchar(10),
             Raw_Field10 varchar(10),
             Detail_DonationDollar int, 
             Detail_DonationDate char(8), 
             Detail_PaymentMethod char(1),
             Detail_DonationChannel char(1)
                );

            INSERT INTO apogee_dedupe_stage
                (ListID, individual_id, company_id, AccountNo, ListCategory01, 
                ListCategory02, ListCategory03, ListCategory04, ListCategory05, Party_Name, 
                IsBlueFlag, CommitteeID, Raw_Field04,Raw_Field05, Raw_Field06, Raw_Field07,
                Raw_Field08,Raw_Field09, Raw_Field10, Detail_DonationDollar, Detail_DonationDate, Detail_PaymentMethod, 
                Detail_DonationChannel)
                SELECT  
                    a.ListID, 
                    a.individual_id, 
                    a.company_id, 
                    a.AccountNo, 
                    a.ListCategory01,
                    a.ListCategory02, 
                    a.ListCategory03, 
                    a.ListCategory04, 
                    a.ListCategory05,
                    b.Party,
                    b.IsBlue, 
                    b.Committee_Id,
                    b.RAW_Field04,
                    b.RAW_Field05,
                    b.RAW_Field06,
                    b.RAW_Field07,
                    b.RAW_Field08,
                    b.RAW_Field09,
                    b.RAW_Field10,
                    b.Detail_DonationDollar, 
                    b.Detail_DonationDate, 
                    b.Detail_PaymentMethod, 
                    b.Detail_DonationChannel 
                FROM	apogee_cust_dedupe_fec a
                JOIN	apogee_trans_dedupe_fec b on a.AccountNo = b.AccountNo;

               DELETE FROM apogee_dedupe_stage
                    WHERE ID NOT IN
                    (
                    SELECT MAX(ID)
                    FROM apogee_dedupe_stage
                    GROUP BY ListID, 
                            AccountNo, 
                            Detail_DonationDate,
                            Detail_DonationDollar
                    );

            """
            redshift_hook.run(sql)

            # insert into child3 temp table
            sql = """
                INSERT INTO apogee_transactions_fec
                (TableID, ListID, individual_id, company_id, AccountNo, ListCategory01, ListCategory02, ListCategory03, 
                ListCategory04, ListCategory05, Party_Name, IsBlueFlag, CommitteeID,
                Raw_Field04, Raw_Field05, Raw_Field06, Raw_Field07, Raw_Field08, Raw_Field09, Raw_Field10,
                Detail_DonationDollar, Detail_DonationDate, Detail_PaymentMethod, Detail_DonationChannel)
                SELECT
                    ID,
                    ListID, 
                    individual_id, 
                    company_id, 
                    AccountNo, 
                    ListCategory01, 
                    ListCategory02, 
                    ListCategory03, 
                    ListCategory04, 
                    ListCategory05, 
                    Party_Name ,
                    IsBlueFlag,
                    CommitteeID,
                    Raw_Field04,
                    Raw_Field05,
                    Raw_Field06,
                    Raw_Field07,
                    Raw_Field08,
                    Raw_Field09,
                    Raw_Field10 ,
                    Detail_DonationDollar, 
                    Detail_DonationDate, 
                    Detail_PaymentMethod,
                    Detail_DonationChannel
                from apogee_dedupe_stage
            """

            if action == "A":
                sql2 = f"""  
                select TableID, sourcelistid, individual_id, company_id, AccountNo, ListCategory01, ListCategory02, ListCategory03, 
                ListCategory04, ListCategory05, Party_Name, IsBlueFlag, CommitteeID,
                Raw_Field04, Raw_Field05, Raw_Field06, Raw_Field07, Raw_Field08, Raw_Field09, Raw_Field10,
                Detail_DonationDollar, Detail_DonationDate, Detail_PaymentMethod, Detail_DonationChannel
                    from apogee_iq_tblchild4 where sourcelistid = {listid}
                """

                sql = sql + " union " + sql2

            redshift_hook.run(sql)

            row = redshift_cursor.fetchone()


def cleanup(buildid, databaseid, *args, **kwargs):
    redshift_hook = get_redshift_hook()
    sql_np = """ DROP TABLE  IF EXISTS apogee_np_transactions_invaliddata; 
            CREATE TABLE apogee_np_transactions_invaliddata (ID bigint, SuppressCode CHAR(1) DEFAULT '');
            
            -- Remove records with Invalid date
            INSERT INTO apogee_np_transactions_invaliddata (ID, SuppressCode)
            SELECT ID, 'I' FROM apogee_transactions_np WHERE ID NOT IN 
            (SELECT ID FROM apogee_np_transactions_invaliddata) 
            AND nvl(apogee_transactions_np.Detail_DonationDate, '') = '';
            
            --Remove records with future dated transactions// GETDATE() + 7 +  4 days for 2 weekends  use ?YYYYMMDD? format.  we will use getdate() + 12.
            INSERT INTO apogee_np_transactions_invaliddata
            SELECT ID, 'F' FROM apogee_transactions_np WHERE ID NOT IN 
            (SELECT ID FROM apogee_np_transactions_invaliddata) 
            AND apogee_transactions_np.Detail_DonationDate > dateadd(day, 12, getdate());
            
            -- Remove records with Aged dated transactions// before 1950 per Matt 2020.02.24.
            INSERT INTO apogee_np_transactions_invaliddata 
            SELECT ID, 'A' FROM apogee_transactions_np WHERE ID NOT IN 
            (SELECT ID FROM apogee_np_transactions_invaliddata) 
            AND apogee_transactions_np.Detail_DonationDate < '19500101';

            -- Remove records with negative or Zero donations
            INSERT INTO apogee_np_transactions_invaliddata
            SELECT ID, 'Z' FROM apogee_transactions_np WHERE ID NOT IN (
            SELECT ID FROM apogee_np_transactions_invaliddata) AND apogee_transactions_np.Detail_DonationDollar <=  0;

            -- Remove SUPPRESSION records by company_id, individual_id
            INSERT INTO apogee_np_transactions_invaliddata 
            SELECT NP.ID, 'S' FROM apogee_transactions_np NP
            WHERE (NP.ID IN (SELECT NP1.ID from apogee_transactions_np NP1 inner join apogee_tblSuppression S on NP1.Company_ID = S.Company_id) 
            OR NP.ID IN (SELECT NP2.ID from apogee_transactions_np NP2 inner join apogee_tblSuppression S on NP2.Individual_ID = S.Individual_ID))
            AND NP.ID NOT IN (SELECT NPI1.ID FROM apogee_np_transactions_invaliddata NPI1);
            
            DROP TABLE  IF EXISTS apogee_np_transactions_invaliddata_backup; 
            SELECT * INTO apogee_np_transactions_invaliddata_backup FROM apogee_transactions_np
                    WHERE ID IN ( SELECT ID FROM apogee_np_transactions_invaliddata); commit;

            DELETE FROM apogee_transactions_np WHERE ID IN ( SELECT ID FROM apogee_np_transactions_invaliddata); 
           
            UPDATE apogee_transactions_np
            SET	ListCategory01 = CASE  ListCategory01
                        WHEN 'NA' THEN 'NT'
                        WHEN 'VE' THEN 'VT'
                        WHEN 'GH' THEN 'HG'
                        WHEN 'LB' THEN 'LI'
                        WHEN 'PO' THEN 'PA'
                        ELSE ListCategory01 END,
                ListCategory02 =  CASE ListCategory02 
                        WHEN 'NA' THEN 'NT'
                        WHEN 'VE' THEN 'VT'
                        WHEN 'GH' THEN 'HG'
                        WHEN 'LB' THEN 'LI'
                        WHEN 'PO' THEN 'PA'
                        ELSE ListCategory02 END;
           """
    redshift_hook.run(sql_np)

    sql_fec = """ DROP TABLE  IF EXISTS apogee_fec_transactions_invaliddata; 
            CREATE TABLE apogee_fec_transactions_invaliddata (ID bigint, SuppressCode CHAR(1) DEFAULT '');

            -- Remove records with Invalid date
            INSERT INTO apogee_fec_transactions_invaliddata (ID, SuppressCode)
            SELECT ID, 'I' FROM apogee_transactions_fec WHERE ID NOT IN 
            (SELECT ID FROM apogee_fec_transactions_invaliddata) 
            AND nvl(apogee_transactions_fec.Detail_DonationDate, '') = '';

            --Remove records with future dated transactions// GETDATE() + 7 +  4 days for 2 weekends  use ?YYYYMMDD? format.  we will use getdate() + 12.
            INSERT INTO apogee_fec_transactions_invaliddata
            SELECT ID, 'F' FROM apogee_transactions_fec WHERE ID NOT IN 
            (SELECT ID FROM apogee_fec_transactions_invaliddata) 
            AND apogee_transactions_fec.Detail_DonationDate > dateadd(day, 12, getdate());

            -- Remove records with Aged dated transactions// before 1950 per Matt 2020.02.24.
            INSERT INTO apogee_fec_transactions_invaliddata 
            SELECT ID, 'A' FROM apogee_transactions_fec WHERE ID NOT IN 
            (SELECT ID FROM apogee_fec_transactions_invaliddata) 
            AND apogee_transactions_fec.Detail_DonationDate < '19500101';

            -- Remove records with negative or Zero donations
            INSERT INTO apogee_fec_transactions_invaliddata
            SELECT ID, 'Z' FROM apogee_transactions_fec WHERE ID NOT IN (
            SELECT ID FROM apogee_fec_transactions_invaliddata) AND apogee_transactions_fec.Detail_DonationDollar <=  0;

            -- Remove SUPPRESSION records by company_id, individual_id
            INSERT INTO apogee_fec_transactions_invaliddata 
            SELECT FEC.ID, 'S' FROM apogee_transactions_fec FEC
            WHERE (FEC.ID IN (SELECT FEC1.ID from apogee_transactions_fec FEC1 inner join apogee_tblSuppression S on FEC1.Company_ID = S.Company_id) 
            OR FEC.ID IN (SELECT FEC2.ID from apogee_transactions_fec FEC2 inner join apogee_tblSuppression S on FEC2.Individual_ID = S.Individual_ID))
            AND FEC.ID NOT IN (SELECT NPI1.ID FROM apogee_fec_transactions_invaliddata NPI1);

            DROP TABLE  IF EXISTS apogee_fec_transactions_invaliddata_backup; 
            SELECT * INTO apogee_fec_transactions_invaliddata_backup
              FROM apogee_transactions_fec WHERE ID IN ( SELECT ID FROM apogee_fec_transactions_invaliddata);
            
            DELETE FROM apogee_transactions_fec WHERE ID IN ( SELECT ID FROM apogee_fec_transactions_invaliddata); 
        """

    redshift_hook.run(sql_fec)


"""

select top 100 * from dba.tblChild3_20073_202009 where sourcelistid = 13614 ; commit;

select top 100 * from dba.tblChild1_20073_202009 ; commit;


select * from DBA.APOGEE_SUMCREATION_A; commit;
select * from DBA.APOGEE_SUMCREATION_B; commit;
select * from DBA.APOGEE_SUMCREATION_C; commit;
select * from DBA.APOGEE_SUMCREATION_D; commit;
select * from DBA.APOGEE_SUMCREATION_E; commit;
select * from DBA.APOGEE_SUMCREATION_FORMULA; commit;
select * from Temp_Formula_ToBeDropped; commit;

	SELECT A.cDESC+B.cDESC+C.cDESC+D.cDESC+E.cDESC AS cFORMULA,iProcessed = 0 
	
	  FROM DBA.APOGEE_SUMCREATION_A A
	INNER JOIN DBA.APOGEE_SUMCREATION_FORMULA F  ON A.ID = F.A_ID
	INNER JOIN DBA.APOGEE_SUMCREATION_B B		ON F.B_ID= B.ID
	INNER JOIN DBA.APOGEE_SUMCREATION_C C		ON F.C_ID = C.ID
	INNER JOIN DBA.APOGEE_SUMCREATION_D D		ON F.D_ID = D.ID
	INNER JOIN DBA.APOGEE_SUMCREATION_E E		ON F.E_ID = E.ID; commit;


"""
