from helpers.redshift import get_redshift_hook
from helpers.sqlserver import get_sqlserver_hook
import pandas as pd
from helpers.excel import *


sqlhook = get_sqlserver_hook()
redshift_hook = get_redshift_hook()

def generateAuditReport(**kwargs):
    global DatabaseID
    if kwargs["dag_run"].conf.get("databaseid") == None:
        raise ValueError("Please pass DatabaseID")
    else:
        DatabaseID = kwargs["dag_run"].conf.get("databaseid")
    return exportToExcel(DatabaseID)


def exportToExcel(DatabaseID):
    # create pivot data sheet in output report
    buildid_sql = f"""SELECT TOP 2 buildid FROM Exclude_ListConversion_Audit WHERE databaseid = {DatabaseID} GROUP BY buildid ORDER BY buildid DESC"""
    buildid_output = redshift_hook.get_records(buildid_sql)
    max_buildid = str(buildid_output[0][0])
    second_max_buildid = str(buildid_output[1][0])

    # ToDo: FILE PAth to be add and delete file if exists
    file_output_path = f"/tmp/Nonprofit_Pivot_AuditReport_{DatabaseID}.xlsx"

    # get listid and listname df from sql server
    sql = f"""SELECT b.ID AS listid, b.cListName AS listname, 
                    CASE a.LK_Action WHEN 'A' THEN 'Add' WHEN 'D' THEN 'Remove' WHEN 'O' THEN 'Reuse' WHEN 'R' THEN 'Replace' WHEN 'N' THEN 'New' ELSE '' END listaction 
                FROM DW_Admin.dbo.tblBuildLoL a (nolock)
               INNER JOIN DW_Admin.dbo.tblMasterLoL b (nolock) 
                  ON a.MasterLoLID = b.ID 
               WHERE BuildID = {max_buildid}"""

    sql_server_df = sqlhook.get_pandas_df(sql)
    # sql_server_df['listid']=sql_server_df['listid'].astype(str)
    
    writer = pd.ExcelWriter(file_output_path)


    redshift_df_pivot_sql = f"""
                    SELECT ValidDonors.listid, 
                        NVL("{second_max_buildid}",0) as ValidDonors_PreviousBuild_{second_max_buildid}, 
                        NVL("{max_buildid}",0) as ValidDonors_NewBuild_{max_buildid},
                        CASE WHEN (NVL("{second_max_buildid}",0))::decimal = 0 
                        THEN round(((NVL("{max_buildid}",0) - NVL("{second_max_buildid}",0))/(NVL("{max_buildid}",0))::decimal)*100) 
                        ELSE round(((NVL("{max_buildid}",0) - NVL("{second_max_buildid}",0))/(NVL("{second_max_buildid}",0))::decimal)*100) END AS "Valid Donors Difference",
                        ValidTrans.iValidTransaction_PreviousBuild_{second_max_buildid},
                        ValidTrans.iValidTransaction_NewBuild_{max_buildid},
                        ValidTrans."valid Transactions Difference",
                        AvgDollar.Avg_Dollar_Amount_PreviousBuild_{second_max_buildid}, 
                        AvgDollar.Avg_Dollar_Amount_NewBuild_{max_buildid},
                        AvgDollar."Avg TransactionDollar Difference",
                        MinDate.Min_Gift_Date_PreviousBuild_{second_max_buildid},
                        MinDate.Min_Gift_Date_NewBuild_{max_buildid},
                        MinDate."Min Gift Date Difference",
                        MaxDate.Max_Gift_Date_PreviousBuild_{second_max_buildid},
                        MaxDate.Max_Gift_Date_NewBuild_{max_buildid},
                        MaxDate."Max Gift Date Difference", 
                        ListCategory.List_Category_01_PreviousBuild_{second_max_buildid},
                        ListCategory.List_Category_01_NewBuild_{max_buildid},
                        ListCategory."List Category Difference"
                    FROM (SELECT buildid, listid, iDonor, iDonorDeceased , iDonorNonMailable  FROM exclude_listconversion_audit WHERE NVL(iDonor, 0) > 0) 
                    PIVOT (SUM(iDonor - iDonorDeceased - iDonorNonMailable) FOR buildid IN ({max_buildid}, {second_max_buildid})) ValidDonors
                    INNER JOIN  
                    (
                        SELECT listid, NVL("{second_max_buildid}",0) as iValidTransaction_PreviousBuild_{second_max_buildid},  NVL("{max_buildid}",0) as iValidTransaction_NewBuild_{max_buildid},
                        CASE WHEN (NVL("{second_max_buildid}",0))::decimal = 0 
                        THEN round(((NVL("{max_buildid}",0) - NVL("{second_max_buildid}",0))/(NVL("{max_buildid}",0))::decimal)*100) 
                        ELSE round(((NVL("{max_buildid}",0) - NVL("{second_max_buildid}",0))/(NVL("{second_max_buildid}",0))::decimal)*100) END AS "valid Transactions Difference"
                        FROM (SELECT buildid, listid, iValidTransaction  FROM exclude_listconversion_audit WHERE NVL(iValidTransaction, 0) > 0) 
                        PIVOT (SUM(iValidTransaction) FOR buildid IN ({max_buildid}, {second_max_buildid}))
                    ) ValidTrans
                    ON ValidDonors.listid = ValidTrans.listid
                    INNER JOIN  
                    (
                        SELECT listid, NVL("{second_max_buildid}",0) as Avg_Dollar_Amount_PreviousBuild_{second_max_buildid},  NVL("{max_buildid}",0) as Avg_Dollar_Amount_NewBuild_{max_buildid},
                        CASE WHEN (NVL("{second_max_buildid}",0))::decimal = 0 
                        THEN round(((NVL("{max_buildid}",0) - NVL("{second_max_buildid}",0))/(NVL("{max_buildid}",0))::decimal)*100) 
                        ELSE round(((NVL("{max_buildid}",0) - NVL("{second_max_buildid}",0))/(NVL("{second_max_buildid}",0))::decimal)*100) END AS "Avg TransactionDollar Difference"
                        FROM (SELECT buildid, listid, iavgtransactiondollar  FROM exclude_listconversion_audit WHERE NVL(iavgtransactiondollar, 0) > 0) 
                        PIVOT (SUM(iavgtransactiondollar) FOR buildid IN ({max_buildid}, {second_max_buildid}))
                    ) AvgDollar
                    ON ValidTrans.listid = AvgDollar.listid
                    INNER JOIN  
                    (
                        SELECT listid, "{second_max_buildid}" AS Min_Gift_Date_PreviousBuild_{second_max_buildid},  "{max_buildid}" AS Min_Gift_Date_NewBuild_{max_buildid},
                        "{max_buildid}" - "{second_max_buildid}" AS "Min Gift Date Difference"
                        FROM (SELECT buildid, listid, dmintransactiondate  FROM exclude_listconversion_audit WHERE dmintransactiondate IS NOT NULL) 
                        PIVOT (MIN(dmintransactiondate) FOR buildid IN ({max_buildid}, {second_max_buildid}))
                    ) MinDate
                    ON ValidTrans.listid = MinDate.listid
                    INNER JOIN  
                    (
                        SELECT listid, "{second_max_buildid}" AS Max_Gift_Date_PreviousBuild_{second_max_buildid},  "{max_buildid}" AS Max_Gift_Date_NewBuild_{max_buildid},
                        "{max_buildid}" - "{second_max_buildid}" AS "Max Gift Date Difference"
                        FROM (SELECT buildid, listid, dmaxtransactiondate  FROM exclude_listconversion_audit WHERE dmaxtransactiondate IS NOT NULL) 
                        PIVOT (MAX(dmaxtransactiondate) FOR buildid IN ({max_buildid}, {second_max_buildid}))
                    ) MaxDate
                    ON ValidTrans.listid = MaxDate.listid
                    INNER JOIN  
                    (
                        SELECT listid, "{second_max_buildid}" AS List_Category_01_PreviousBuild_{second_max_buildid},  "{max_buildid}" AS List_Category_01_NewBuild_{max_buildid},
                        case when "{max_buildid}" = "{second_max_buildid}" then 'False' else 'True' end as "List Category Difference"
                        FROM (SELECT buildid, listid, clistcategory01  FROM exclude_listconversion_audit WHERE clistcategory01 IS NOT NULL) 
                        PIVOT (MAX(clistcategory01) FOR buildid IN ({max_buildid}, {second_max_buildid}))
                    ) ListCategory
                    ON ValidTrans.listid = ListCategory.listid
                    """


    redshift_df_pivot_df = redshift_hook.get_pandas_df(redshift_df_pivot_sql)
    df = redshift_df_pivot_df


            
    pivot_report_df = pd.merge(
        df,
        sql_server_df,
        left_on=["listid"],
        right_on=["listid"],
        how="left",
    )[
        [
            "listid",
            "listname",
            "listaction",
            f"validdonors_previousbuild_{second_max_buildid}",
            f"validdonors_newbuild_{max_buildid}",
            "valid donors difference",
            f"ivalidtransaction_previousbuild_{second_max_buildid}",
            f"ivalidtransaction_newbuild_{max_buildid}",
            "valid transactions difference",
            f"avg_dollar_amount_previousbuild_{second_max_buildid}",
            f"avg_dollar_amount_newbuild_{max_buildid}",
            "avg transactiondollar difference",
            f"min_gift_date_previousbuild_{second_max_buildid}",
            f"min_gift_date_newbuild_{max_buildid}",
            f"min gift date difference",
            f"max_gift_date_previousbuild_{second_max_buildid}",
            f"max_gift_date_newbuild_{max_buildid}",
            f"max gift date difference",
            f"list_category_01_previousbuild_{second_max_buildid}",
            f"list_category_01_newbuild_{max_buildid}",
            "list category difference"
        ]
    ].fillna(0)  # join df's
    
    

    # # sort by list name
    pivot_report_df.sort_values(by=["listname"], inplace=True)

    pivot_report_df.rename(
        columns={
            "listid": "List ID",
            "listname": "List Name",
            "listaction": "List Action",
            f"validdonors_previousbuild_{second_max_buildid}": f"Valid Donors Previous Build - {second_max_buildid}",
            f"validdonors_newbuild_{max_buildid}": f"Valid Donors New Build - {max_buildid}",
            "valid donors difference": "Valid Donors Difference",
            f"ivalidtransaction_previousbuild_{second_max_buildid}": f"Valid Gifts Previous Build - {second_max_buildid}",
            f"ivalidtransaction_newbuild_{max_buildid}": f"Valid Gifts New Build - {max_buildid}",
            "valid transactions difference": "Valid Gifts Difference",
            f"avg_dollar_amount_previousbuild_{second_max_buildid}": f"Avg $ Amount Previous Build - {second_max_buildid}",
            f"avg_dollar_amount_newbuild_{max_buildid}": f"Avg $ Amount New Build - {max_buildid}",
            "avg transactiondollar difference": "Avg $ Difference",
            f"min_gift_date_previousbuild_{second_max_buildid}": f"Min Gift Date Previous Build - {second_max_buildid}",
            f"min_gift_date_newbuild_{max_buildid}": f"Min Gift Date New Build - {max_buildid}",
            f"min gift date difference": "Min Gift Date Difference",
            f"max_gift_date_previousbuild_{second_max_buildid}": f"Max Gift Date Previous Build - {second_max_buildid}",
            f"max_gift_date_newbuild_{max_buildid}": f"Max Gift Date New Build - {max_buildid}",
            "max gift date difference": "Max Gift Date Difference"
            , f"list_category_01_previousbuild_{second_max_buildid}" : f"List_Category_01_PreviousBuild_{second_max_buildid}",
            f"list_category_01_newbuild_{max_buildid}" : f"List_Category_01_NewBuild_{max_buildid}",
            "list category difference": "List Category Difference"
        },
        inplace=True,
    )
    
    add_thousand_separator(pivot_report_df)
    pivot_report_df['List ID'] = pivot_report_df['List ID'].str.replace(',', '')

    pivot_report_df.to_excel(
        writer,
        sheet_name="PivotTable",
    )

    # Auto-adjust columns' width
    set_column_width(pivot_report_df, "PivotTable", writer)
    writer.save()
    autoFilter(file_output_path)

