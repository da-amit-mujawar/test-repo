import json
import logging
import sys

from pyspark.sql import SparkSession

import commonlib as cl


def createOuterJoin(ps_base_tbl, ps_base_col, ps_curr_tbl, ps_curr_col):
    logger.info("Create Outer Join ")
    ls_cmp_id = "COALESCE( " + ps_base_col + ", " + ps_curr_col + ") as Process_ID, "
    ls_From = " FROM " + ps_base_tbl
    ls_Join = " FULL OUTER JOIN " + ps_curr_tbl
    ls_JoinKey = " ON " + ps_base_tbl + "." + ps_base_col + " = " + ps_curr_tbl + "." + ps_curr_col
    ls_Order = " ORDER BY COALESCE( " + ps_base_col + ", " + ps_curr_col + ") "
    ls_final_sql = "SELECT " + ls_cmp_id + " * " + ls_From + ls_Join + ls_JoinKey + ls_Order
    logger.info("Outer Join SQL: {0}".format(ls_final_sql))
    df_cat_tbl = spark.sql(ls_final_sql)
    df_cat_tbl = df_cat_tbl.withColumnRenamed(ps_base_col, "ToDrop") \
        .withColumnRenamed("Process_ID", ps_base_col).drop("ToDrop").drop(ps_curr_col)
    return df_cat_tbl


# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName("ListCalculation").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s: %(message)s')

# Setup Variables.
logger.info("Setting up environment variables")
ls_content = str(sys.argv[1]).replace("'", '"')
lj_content = json.loads(ls_content)

pd_process_date = sys.argv[2]

# curDate = "trunc(to_date('" + pd_process_date + "','yyyyMMdd'), 'month') + 5"
curDate = "to_date('" + pd_process_date + "','yyyyMMdd') + 5"

ls_base = lj_content.get("base_location")
ls_code = lj_content.get("code_base")
ls_app_name = lj_content.get("app_name")
ls_config = ls_code + lj_content.get("config_location") + ls_app_name + "/"

logger.info("Starting Read Input Process")
ls_prcs_loc = ls_base + lj_content.get("process_location")
ls_rej_loc = ls_base + lj_content.get("reject_location")
ls_dsc_loc = ls_base + lj_content.get("discard_location")
ls_temp_loc = ls_base + lj_content.get("temp_location")
ls_out_loc = ls_base + lj_content.get("output_location")
ls_iam_role = '{iam_role}'
ls_input = ls_base + "input/"
ls_outtmp = ls_temp_loc + "Custom/"
logger.info("Location - Temp: {0}".format(ls_outtmp))
ls_input_json = ls_config + "input_layout.json"

logger.info("Starting Read Raw Transactions")
df_trans = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_Trans")
df_trans.createOrReplaceTempView("NP_Transactions")

# LTD Data
logger.info("Starting Read LTD Data")
df_LTD = cl.fn_file_2_df(logger, spark, ls_out_loc + "Phase1")
df_LTD.createOrReplaceTempView("tblLTD")
# Read the Parquet file and continue as transaction input file - End

next_step = "Dimension"

if next_step == "Dimension":
    la_cols = ["col.Code", "col.ID", "col.Query"]
    dfA = cl.json_2_df(spark, ls_config + "CustomQueries.json", la_cols)
    dfA.createOrReplaceTempView('tblCustom')
    logger.info("Prefix Custom Queries Count: {0}".format(dfA.count()))
    next_step = "Process"

if next_step == "Process":
    lv_code_list = spark.sql("SELECT DISTINCT Code FROM tblCustom ORDER BY Code").collect()
    la_tbllist = []
    for rec in lv_code_list:
        Code = str(rec['Code'])
        lv_sql = "  SELECT DISTINCT ID, Code || '_' || ID CodeID, Query " \
                 "    FROM tblCustom " \
                 "   WHERE Code = '{0}' " \
                 "ORDER BY id".format(Code)
        for run in spark.sql(lv_sql).collect():
            print(str(run['Query']))
            ls_tblname = str(run['CodeID'])
            ls_col_name = ls_tblname + "_Process_ID"
            lv_cstm_sql = str(run['Query'])
            try:
                df_Custom = spark.sql(lv_cstm_sql) \
                    .withColumnRenamed("Process_ID", ls_col_name)
                cl.showdata(logger, spark, df_Custom)
                la_tbllist.append(ls_tblname + "." + ls_col_name)
                cl.write_2_file(logger, df_Custom, ls_outtmp + "CustomQuery_" + ls_tblname)
                df_Custom = cl.fn_file_2_df(logger, spark, ls_outtmp + "CustomQuery_" + ls_tblname)
                df_Custom.createOrReplaceTempView(ls_tblname)
                df_Custom.printSchema()
            except:
                logger.error("Error Processing SQL []".format(lv_cstm_sql))

    #  Outer Join all Category Staging tables - Start
    ltbl = la_tbllist[0].split('.')
    ls_FTbl = ltbl[0]
    ls_FCol = ltbl[1]
    for curList in la_tbllist[1:]:
        ls_cur_col = curList.split('.')[1]
        ls_cmp_id = "COALESCE( " + ls_FCol + ", " + ls_cur_col + ") as Process_ID, "
        ls_cur_tbl = curList.split('.')[0]
        ls_From = " FROM {0} FULL OUTER JOIN {2} ON {0}.{1} = {2}.{3}" \
            .format(ls_FTbl, ls_FCol, ls_cur_tbl, ls_cur_col)
        ls_final_sql = "SELECT " + ls_cmp_id + " * " + ls_From + " ORDER BY COALESCE( " + ls_FCol + ", " + ls_cur_col + ") "
        logger.info(ls_final_sql)
        df_oj_tbl = spark.sql(ls_final_sql)
        df_oj_tbl = df_oj_tbl.withColumnRenamed(ls_FCol, "ToDrop") \
            .withColumnRenamed("Process_ID", ls_FCol).drop("ToDrop").drop(ls_cur_col)
        cl.write_2_file(logger, df_oj_tbl, ls_outtmp + "CustomJoin_" + ls_cur_tbl)
        df_oj_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "CustomJoin_" + ls_cur_tbl)
        df_oj_tbl.createOrReplaceTempView(ls_FTbl)
    #  Outer Join all Category Staging tables - End

    df_oj_tbl = df_oj_tbl.withColumnRenamed(ls_FCol, lj_content.get("KeyColumnName"))

    ls_output_loc = ls_out_loc + "CustomQuery"
    cl.write_2_file(logger, df_oj_tbl, ls_output_loc)

    # Creating table structure in RedShift
    logger.info("Create SQL file for RedShift Table Structure")
    ls_bucket_name = ls_out_loc.lstrip('s3://').split('/')[0]
    ls_sql_loc = '/'.join(ls_out_loc.lstrip('s3://').split('/')[1:]) + "sql/"
    ls_target_tbl = lj_content.get("tblCustom")

    df_Category = cl.fn_file_2_df(logger, spark, ls_output_loc)
    ld_script = {"schema_name": "public",
                 "table_name": ls_target_tbl,
                 "bucket_name": ls_bucket_name,
                 "output_path": ls_sql_loc,
                 "data_location": ls_output_loc,
                 "key_column": lj_content.get("KeyColumnName"),
                 "key_col_type": lj_content.get("KeyColumnType", "VARCHAR(100)"),
                 }
    cl.gen_ddl_script(logger, df_Category.schema.fields, ld_script)
    logger.info("SQL file for RedShift Table Structure created successfully")

    logger.info("Completing Custom Attributes Processing")

logger.info("Custom Attributes Processing completed successfully")
