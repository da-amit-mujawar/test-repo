from datetime import datetime
from airflow.models import Variable
from airflow.hooks.mssql_hook import MsSqlHook
from contextlib import closing
from helpers.redshift import *
from helpers.sqlserver import *
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp
import pandas as pd
from pandas.io import sql as psql
from helpers.s3 import save_dataframe
from helpers.s3 import read_file
from helpers.s3 import getObjectList
from reports.list_updates_mgen_audit_report.auditField import AuditField
import copy
import glob
import os
import sys
from os.path import dirname, abspath
from airflow.hooks.postgres_hook import PostgresHook

file_output_path = '/tmp/'
mgen_count_child_s3_bucket = 'develop_idms-7933-internalfiles' #'develop_idms-2722-playground'
mgen_count_child_s3_path = 'neptune/mGen' #'devansh/mgen'
def generateAuditReport(buildId, **kwargs):
    buildIdToProceed = buildId
    if buildId is None:
        buildIdToProceed = kwargs['dag_run'].conf.get('buildId')
        print("Build ID to proceed with is ", buildIdToProceed)

    databaseId = collectAuditReportData(buildIdToProceed)
    if databaseId is None:
        return False
    return exportToExcel(buildIdToProceed, databaseId)


def exportToExcel(buildId, databaseId):
    # ToDo: FILE PAth to be add and delete file if exists
    auditReportTable = f"Common.dbo.tbl9900_{databaseId}"
    auditReportSummaryTable = f"Common.dbo.tbl9900_{databaseId}_Summary"
    # summaryDf = pd.DataFrame({"Build": [], "Count": []})
    # pivotDataDf = pd.DataFrame({"cTable": [], "cField": [], "nTotalCount": [], "cValue": []})
    file_output_path = f'/tmp/Mgen_AuditReport_{databaseId}_{buildId}.xlsx'
    writer = pd.ExcelWriter(file_output_path)
    sqlHook = get_sqlserver_hook()
    sql = f"""SELECT cBuild AS Build, nTotalCount AS [Count], CASE WHEN nIndividualCount = 0 THEN null ELSE nIndividualCount END AS [Individuals], CASE WHEN nCompanyCount = 0 THEN null ELSE nCompanyCount END AS [Sites or Households] FROM {auditReportSummaryTable} ORDER BY 1 DESC"""
    summaryDf = sqlHook.get_pandas_df(sql)[['Build', 'Count']]
    summaryDf.to_excel(writer, sheet_name="Summary")
    # print("Summary Data", summaryDf)

    # pivotDataDf = sqlHook.get_pandas_df(sql)
    # pivotDataDf.to_excel(writer, sheet_name="PivotData")
    # print("Pivot Data", pivotDataDf)
    # pivotTableDf = summaryDf.pivot(index="cTable", columns="cField", values="cValue")
    pivotDataResult = sqlHook.get_pandas_df(getPivotSql(auditReportTable, buildId))[["Build", "Field Name", "Field "
                                                                                                            "Value",
                                                                                     "Count", "Value Description"]]
    # print("Pivot Data", pivotDataResult)
    enrichDescription(buildId, pivotDataResult, 'ARR_12M', 'cValue', '12M')
    enrichDescription(buildId, pivotDataResult, 'ARR_3M', 'cValue', '3M')
    enrichDescription(buildId, pivotDataResult, 'ARR_LT_A', 'cValue', 'LT')
    enrichDescription(buildId, pivotDataResult, 'ARR_LT_B', 'cValue', 'LT')
    pivotDataResult.to_excel(writer, sheet_name="PivotData")

    pivotTableDf = pd.pivot_table(pivotDataResult, index=["Field Name", "Field Value", "Value Description"],
                                  columns=["Build"], values=["Count"])
    last2Columns = pivotTableDf[pivotTableDf.columns[-2:]].copy()
    diff = []
    for index, row in last2Columns.iterrows():
        # print(">>>", row[0], " >> ", row[1])
        val1 = 0
        val2 = 0
        if row[0] is not None and str(row[0]) != '':
            try:
                val1 = int(row[0])
            except:
                print("Failed to convert value to integer -", row[0], "-")
        if row[1] is not None and str(row[1]) != '':
            try:
                val2 = int(row[1])
            except:
                print("Failed to convert value to integer -", row[1], "-")
        difference = 0
        if val1 == 0 and val2 > 0:
            difference = 100
        elif val1 > 0 and val2 == 0:
            difference = -100
        elif val1 == 0 and val2 == 0:
            difference = 0
        else:
            difference = "{:.2f}".format(((val2 - val1) / (val1 if val1 > 0 else 1)) * 100)
        # difference = difference + '%'
        diff.append(f'{difference}%')
    pivotTableDf["Difference"] = diff
    pivotTableDf.to_excel(writer, sheet_name="PivotTable")
    # print("Pivot Table", pivotTableDf)
    writer.save()


def getPivotSql(auditReportTable, buildId):
    return f"""SELECT RIGHT(a.cTable,6) as Build, UPPER(a.cField) AS "Field Name", LTRIM(RTRIM(UPPER(a.cValue))) AS "Field Value", nTotalCount AS [Count], RTRIM(ISNULL(UPPER(LEFT(dd.cDescription,50)), '')) as "Value Description" FROM {auditReportTable} a LEFT JOIN (SELECT cFieldName, cValue, MAX(cDescription) AS cDescription FROM DW_Admin.dbo.tblBuildTable b INNER JOIN DW_Admin.dbo.tblBuildTableLayout c ON b.ID = c.BuildTableID INNER JOIN DW_Admin.dbo.tblBuildDD d ON d.BuildTableLayoutID = c.ID WHERE b.BuildID = {buildId} GROUP BY cFieldName, cValue) AS DD ON a.cField = dd.cFieldName AND a.cValue = dd.cValue WHERE RIGHT(a.cTable,6) IN (SELECT TOP 13 RIGHT(cTable,6) FROM {auditReportTable} GROUP BY RIGHT(cTable,6) ORDER BY 1 DESC) AND a.cValue <> '' ORDER BY 1, 2, 3"""


def collectAuditReportData(buildId):
    sqlHook = get_sqlserver_hook()
    sql = f"""SELECT Top 1 * FROM tblBuild WHERE ID = {buildId}"""
    # print(sql)
    buildTableData = sqlHook.get_pandas_df(sql).iloc[0]
    auditTable = None
    if buildTableData is not None:
        # print(buildTableData)
        databaseID = buildTableData.DatabaseID
        buildTableName = f"tblMain_{buildId}_{buildTableData.cBuild}"
        auditReportSummaryTable = f"""Common.dbo.tbl9900_{databaseID}_Summary"""
        auditReportTable = f"""Common.dbo.tbl9900_{databaseID}"""
        auditFieldList = getAuditFields(databaseID, buildTableData.ID, buildTableData.cBuild)
        sql = f"""DELETE FROM {auditReportTable} WHERE cTable = '{buildTableName}'""";
        sqlHook.run(sql)
        # Adding data to addToSummary table
        addToSummary(databaseID, buildTableName, buildTableData.cBuild, auditReportSummaryTable)

        redshift_hook = get_redshift_hook()
        redshift_conn = redshift_hook.get_conn()
        redshift_conn.autocommit = (True)
        for auditField in auditFieldList:
            if auditField.fieldType == 'S':
                sql = f"""SELECT '{buildTableName}' AS cTable, '{auditField.fieldName}' AS cField, CAST(COUNT(*) AS Int) nTotalCount, cast({auditField.fieldName} as varchar) AS cValue FROM {auditField.tableName} GROUP BY cValue ORDER BY cValue"""
                # print(">>>>", sql)
                try:
                    auditTable = redshift_hook.get_pandas_df(sql)
                except:
                    print("Failed to run query:", sql)
            else:
                auditTable = collectMultiValueAuditReportData(buildTableName, auditField.fieldName,
                                                              auditField.tableName)
            # BULK COPY TO BE PLACED HERE
            if auditTable.shape is not None and auditTable.shape[0] > 0:
                for index, row in auditTable.iterrows():
                    sql = f"""insert into {auditReportTable} (ctable, cfield, nTotalCount, cvalue) values ('{row.ctable}', '{row.cfield}', '{row.ntotalcount}', '{row.cvalue}')"""
                    sqlHook.run(sql)
        if int(databaseID) == 847:
            collectMultiValueAuditReportData_847(buildTableName, auditReportTable)
        elif int(databaseID) == 65 or int(databaseID) == 66:
            sql = f"""DELETE FROM {auditReportTable} WHERE cfield = 'LISTID' AND CAST(cValue AS Int) < 0"""
            sqlHook.run(sql)

        return databaseID
    else:
        return None


def collectMultiValueAuditReportData_847(buildTableName, auditReportTable):
    files = getFileListFromS3(mgen_count_child_s3_bucket, mgen_count_child_s3_path)
    files = [x for x in files if x.startswith(f"{mgen_count_child_s3_path}/Count_Child")]

    print("mgen_count_child files", files)
    df = pd.DataFrame({"cTable": [], "cField": [], "nTotalCount": [], "cValue": []})
    # print(df)
    sqlHook = get_sqlserver_hook()
    for file in files:
        eachField = file[file.rindex("/")+1:].lower().strip("\n")
        fieldName = eachField[14:len(eachField)-4]
        # print(fieldName)
        # print("Reading data for file ", file)
        fileReader = readFileFromS3(mgen_count_child_s3_bucket, file)
        #print("Reading data ", fileReader)
        lines = fileReader.split('\n')
        #print("lines from file ", lines)
        for line in lines:
            # Next line to read
            # print(line)
            eachline = line
            eachline = eachline.strip("\r")
            splits = eachline.split("|")
            # print("splits ", splits)
            if len(splits) == 2:
                data = {"cTable": buildTableName, "cField": fieldName, "nTotalCount": splits[1], "cValue": splits[0]}
                df = df.append(data, ignore_index=True)
            #line = fileReader.readline()
    # pivot = df.pivot(index="nTotalCount", columns="cField", values="cValue")
    # print(pivot)

    # Bulk insert to auditReportTable from df
    if df.shape is not None and df.shape[0] > 0:
        for index, row in df.iterrows():
            sql = f"""insert into {auditReportTable} (ctable, cfield, nTotalCount, cvalue) values ('{row.cTable}', '{row.cField}', '{row.nTotalCount}', '{row.cValue}')"""
            sqlHook.run(sql)
    # df.to_excel("test.xlsx", index=False)
    return True


def getAuditFields(databaseId, buildId, cBuild):
    auditFields = []
    file = open(os.path.abspath(os.path.dirname(__file__)) + f"/audit_fields/Fields_{databaseId}", "r")
    line = file.readline()
    count = 0
    while line:
        line = line.strip("\n")
        # print(line)
        auditField = None
        literals = line.split(",")
        literalsCount = len(literals)
        if literalsCount == 2:
            auditField = AuditField(literals[0].format(buildId, cBuild), literals[1], None)
        elif literalsCount == 3:
            auditField = AuditField(literals[0].format(buildId, cBuild), literals[1], literals[2])
        else:
            print(f"Invalid audit field format for: -{line}-")
        # print("Audit Field: ", auditField)
        if auditField:
            auditFields.append(auditField)
        count += 1
        # Next line to read
        line = file.readline()
    print(f"Number of lines in Fields_{databaseId} file are {count}")
    return set(auditFields)


def addToSummary(databaseId, buildTableName, build, auditReportSummaryTable):
    sqlHook = get_sqlserver_hook()
    sql = f"""DELETE FROM {auditReportSummaryTable} WHERE cBuild = '{build}'"""
    sqlHook.run(sql)

    file = open(os.path.abspath(os.path.dirname(__file__)) + f"/summary_sql/SummarySQL_{databaseId}", "r")
    sqlLine = file.readline().format(buildTableName)
    # print(sqlLine)
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)
    result = redshift_hook.get_pandas_df(sqlLine).iloc[0]
    # print(">>>>>>>", result)
    sql = f"""INSERT INTO {auditReportSummaryTable} VALUES ( {build}, {result.ntotalcount}, {result.nindividualcount}, {result.ncompanycount})"""
    # print("Inset statement: ", sql)

    sqlHook.run(sql)


def collectMultiValueAuditReportData(buildTableName, fieldName, tableName):
    sqlHook = get_sqlserver_hook()
    redshift_hook = get_redshift_hook()
    redshift_conn = redshift_hook.get_conn()
    redshift_conn.autocommit = (True)
    auditTable = None
    sql = f"""SELECT DISTINCT a.cValue FROM tblBuildDD A INNER JOIN tblBuildTableLayout B ON A.BuildTableLayoutID = B.ID INNER JOIN tblBuildTable C ON c.ID = B.BuildTableID WHERE c.cTableName = '{tableName}' AND B.cFieldName = '{fieldName}'"""
    # print(">>collectMultiValueAuditReportData>>", sql)
    ddTable = sqlHook.get_pandas_df(sql)
    # print(ddTable)
    for index, row in ddTable.iterrows():
        # print(">>collectMultiValueAuditReportData each row >>", row)
        value = row.cValue
        sql = f"""SELECT '{buildTableName}' AS cTable, '{fieldName}' AS cField, CAST(COUNT(*) AS Int) nTotalCount, {value} AS cValue FROM {tableName} WHERE {fieldName} LIKE '%,{value},%' """
        # print(sql)
        auditTableSingle = redshift_hook.get_pandas_df(sql)
        # print("auditTableSingle >>", auditTableSingle)
        if auditTable is None:
            auditTable = copy.deepcopy(auditTableSingle)
        # auditTable = pd.merge(auditTable, auditTableSingle, on=['ctable', 'cfield'])
        auditTable = auditTable.append(auditTableSingle)
    return auditTable

def enrichDescription(buildId, pivotDataResult, fieldName, cFieldName, capsDescriptionLike):
    sql = f"""SELECT cast(cValue as varchar) AS cValue , cDescription FROM DW_Admin.dbo.tblBuildTable b INNER JOIN DW_Admin.dbo.tblBuildTableLayout c ON b.ID = c.BuildTableID INNER JOIN DW_Admin.dbo.tblBuildDD d ON d.BuildTableLayoutID = c.ID WHERE b.BuildID = {buildId} and cFieldName = '{cFieldName}' and upper(cDescription)  like '%{capsDescriptionLike}' """
    sqlHook = get_sqlserver_hook()
    descriptionKeyVal = sqlHook.get_pandas_df(sql)[['cValue', 'cDescription']]
    rowIndexes = pivotDataResult.index[pivotDataResult['Field Name'] == fieldName].tolist()
    if( len(rowIndexes) > 0 ):
        for idx in rowIndexes:
            lookupIndex = descriptionKeyVal.index[
                descriptionKeyVal['cValue'] == pivotDataResult.iloc[idx]["Field Value"]].tolist()
            # Updating "Value Description" of pivot table with equivalent match
            if ( len(lookupIndex) > 0 ):
                pivotDataResult.iloc[[idx],[4]] = descriptionKeyVal.iloc[lookupIndex[0]]["cDescription"]

def getFileListFromS3(bucket, path):
    files = getObjectList(bucket, path, 100)
    # print("getFileListFromS3 files", files)
    return files

def readFileFromS3(bucket, file):
    result = read_file(bucket, file)
    # print( "readFileFromS3", result.split("\n"))
    return result
