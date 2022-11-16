import json
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col, row_number, lit, monotonically_increasing_id
from pyspark.sql.window import Window

import commonlib as cl

# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName("PrepInputData").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s: %(message)s')

# Setup Variables.
logger.info("Setting up environment variables")
ls_content = str(sys.argv[1]).replace("'", '"')
lj_content = json.loads(ls_content)
pd_process_date = sys.argv[2]

logger.info(lj_content)
# curDate = "trunc(to_date('" + pd_process_date + "','yyyyMMdd'), 'month') + 5"
curDate = "to_date('" + pd_process_date + "','yyyyMMdd') + 5"

ls_base = lj_content.get("base_location")
ls_code = lj_content.get("code_base")
ls_catcols = lj_content.get("CatColumnList", "ListCategory01")
ls_app_name = lj_content.get("app_name")
ls_config = ls_code + lj_content.get("config_location") + ls_app_name + "/"

logger.info("Starting Read Input Process")
ls_prcs_loc = ls_base + lj_content.get("process_location")
ls_rej_loc = ls_base + lj_content.get("reject_location")
ls_dsc_loc = ls_base + lj_content.get("discard_location")
ls_temp_loc = ls_base + lj_content.get("temp_location")
ls_out_loc = ls_base + lj_content.get("output_location")

if len(lj_content.get("ExtraColumns-Trans")) > 0:
    ls_ExtraTrans = ',' + str(lj_content.get("ExtraColumns-Trans")).strip(',')
else:
    ls_ExtraTrans = ''

if len(lj_content.get("ExtraColumns-Category")) > 0:
    ls_ExtraCategory = ',' + str(lj_content.get("ExtraColumns-Category")).strip(',')
else:
    ls_ExtraCategory = ''

if len(lj_content.get("ExtraColumns-List")) > 0:
    ls_ExtraList = ',' + str(lj_content.get("ExtraColumns-List")).strip(',')
else:
    ls_ExtraList = ''
next_step = 'ReadData'

if next_step == 'ReadData':
    lf_datafile = cl.fn_file_2_df(logger, spark, ls_prcs_loc)
    lf_datafile.createOrReplaceTempView("stg_transactions")

    logger.info("Starting Parquet File for Transaction Processing.")
    lv_stg_sql = "SELECT Table_ID, Process_ID, ListID, {2}, DonationDate, " \
                 "       CAST(ROUND(DonationDollar,0) AS INT) DonationDollar, PaymentMethod,   " \
                 "       CASE WHEN LENGTH(LTRIM(DonationChannel)) = 0 THEN NULL ELSE LTRIM(DonationChannel) END DonationChannel, " \
                 "       CAST(CEIL(months_between({0}, DonationDate )) as INT) MB_DonationDate " \
                 "       {1} " \
                 "  FROM stg_transactions df_final ".format(curDate, ls_ExtraTrans, ls_catcols)
    df_trans = spark.sql(lv_stg_sql)

    cl.write_2_file(logger, df_trans, ls_temp_loc + "NP_Trans")
    df_trans = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_Trans")
    df_trans.createOrReplaceTempView("NP_Transactions")
    logger.info("Parquet File for Transaction Processing completed successfully.")

    # Category Base Data
    logger.info("Starting Parquet File for Category Processing.")
    ls_whr_nn = ""
    ls_whr_g0 = ""
    for rec in str(ls_catcols).split(','):
        ls_whr_nn = ls_whr_nn + "{0} IS NOT NULL OR ".format(rec)
        ls_whr_g0 = ls_whr_g0 + "length(ltrim({0})) > 0 OR ".format(rec)
    ls_whr_nn = ls_whr_nn.rstrip('OR ')
    ls_whr_g0 = ls_whr_g0.rstrip('OR ')

    lv_cat_sql = "SELECT Process_ID, {1}, DonationDollar, DonationChannel, " \
                 "       PaymentMethod, DonationDate, MB_DonationDate {0}  " \
                 "  FROM NP_Transactions " \
                 " WHERE ({2}) " \
                 "   AND ({3}) ".format(ls_ExtraCategory, ls_catcols, ls_whr_nn, ls_whr_g0)
    df_catgy = spark.sql(lv_cat_sql).repartition(ls_catcols.split(',')[0])
    cl.write_2_file(logger, df_catgy, ls_temp_loc + "NP_TransCat")
    # df_catgy = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_TransCat")
    logger.info("Parquet File for Category Processing completed successfully.")

    # List Base Data
    logger.info("Starting Parquet File for List Processing.")
    lv_lst_sql = "SELECT Process_ID, ListID, DonationDollar, DonationChannel, PaymentMethod, " \
                 "       DonationDate, MB_DonationDate {0} " \
                 "  FROM NP_Transactions " \
                 " WHERE ListID IS NOT NULL AND length(ltrim(ListID)) > 0".format(ls_ExtraList)
    df_list = spark.sql(lv_lst_sql).repartition("ListID")
    cl.write_2_file(logger, df_list, ls_temp_loc + "NP_TransList")
    # df_list = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_TransList")
    logger.info("Parquet File for List Processing completed successfully.")
    next_step = 'WriteOutput'

if next_step == 'WriteOutput':
    ls_target_tbl = lj_content.get("tblRaw")
    ls_key_column = lj_content.get("KeyColumnName")

    # Create a data frame to use KeyColumn Name
    lv_raw_sql = "SELECT * FROM stg_transactions ORDER BY Process_ID"
    df_rawdata = spark.sql(lv_raw_sql) \
        .withColumnRenamed("Process_ID", ls_key_column) \
        .withColumnRenamed('ListID', 'SourceListID') \
        .withColumnRenamed('Account_No', 'AccountNo') \
        .withColumnRenamed('DonationDate', 'Detail_DonationDate') \
        .withColumnRenamed('DonationDollar', 'Detail_DonationDollar') \
        .withColumnRenamed('DonationChannel', 'Detail_DonationChannel') \
        .withColumnRenamed('PaymentMethod', 'Detail_PaymentMethod') \
        .withColumn("ID", monotonically_increasing_id() + 1) \
        .withColumn("ID", col("ID").cast("long"))

    cl.write_2_file(logger, df_rawdata, ls_out_loc + "RawData")
    # Dataframe for Raw Data ends here.

    ls_bucket_name = ls_out_loc.lstrip('s3://').split('/')[0]
    ls_sql_loc = '/'.join(ls_out_loc.lstrip('s3://').split('/')[1:]) + "sql/"

    ld_script = {"schema_name": "public",
                 "table_name": ls_target_tbl,
                 "bucket_name": ls_bucket_name,
                 "output_path": ls_sql_loc,
                 "data_location": ls_out_loc + "RawData",
                 "key_column": lj_content.get("KeyColumnName"),
                 "key_col_type": lj_content.get("KeyColumnType", "VARCHAR(100)"),
                 "column_prefix": str(lj_content.get("ColumnPrefix", "")).strip(' '),
                 "column_suffix": str(lj_content.get("ColumnSuffix", "")).strip(' ')
                 }
    # cl.gen_ddl_script(logger, df_rawdata.schema.fields, "public", ls_target_tbl, ls_bucket_name,
    #                   ls_sql_loc, ls_out_loc + "RawData")
    cl.gen_ddl_script(logger, df_rawdata.schema.fields, ld_script)

    logger.info("SQL file for RedShift Table Structure created successfully")

logger.info("Prep Input Data Successfully completed.")
