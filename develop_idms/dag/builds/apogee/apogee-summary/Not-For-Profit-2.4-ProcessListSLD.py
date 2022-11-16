import json
import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

import commonlib as cl

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def with_columns_renamed(fun):
    def _(df):
        cols = list(
            map(
                lambda col_name: col("`{0}`".format(col_name)).alias(fun(col_name)),
                df.columns,
            )
        )
        return df.select(*cols)

    return _


def rename_columns(s):
    return s.replace(s, "M48_" + s + "_Since_Last_Donation")


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s : %(levelname)s: %(message)s"
)

# Setup Variables.
logger.info("Setting up Since Last Donation Calculation")
logger.info("Setting up environment variables")
ls_content = str(sys.argv[1]).replace("'", '"')
lj_content = json.loads(ls_content)

pd_process_date = sys.argv[2]
pl_list = sys.argv[3]

# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName(
    "ListCalculation-{0}".format(pl_list)
).getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

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
ls_input = ls_base + "input/"
ls_outtmp = ls_temp_loc + "List-{0}/".format(pl_list)
logger.info("Location - Temp: {0}".format(ls_outtmp))
ls_input_json = ls_config + "input_layout.json"
ls_keycolname = lj_content.get("KeyColumnName")

df_trans = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_Trans")
df_trans.createOrReplaceTempView("NP_Transactions")

# List Base Data
df_list = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_TransList")
df_list.createOrReplaceTempView("NP_TransList")

transform_df = spark.sql(
    "SELECT Process_ID AS Process_ID, LISTID, MB_DonationDate from NP_TransList WHERE MB_DonationDate  BETWEEN 0 AND 48   AND length(ListID) > 0  GROUP BY Process_ID, LISTID, MB_DonationDate"
)

logger.info("Pivoting data started")
pivot_df = (
    transform_df.groupBy("Process_ID").pivot("listid").min("MB_DonationDate").na.fill(0)
)
logger.info("Pivoting data completed")

logger.info("Adding delta list ids started")
listid = df_trans.select("listid").distinct().rdd.flatMap(lambda x: x).collect()
pivotlistid = pivot_df.drop("Process_ID").columns
deltalistid = set(listid) - set(pivotlistid)
all_list_df = pivot_df.select(["*"] + [lit(0).alias(f"{x}") for x in deltalistid])
logger.info("Adding delta list ids completed")

rename_df = with_columns_renamed(rename_columns)(all_list_df)
since_last_donation_df = rename_df.withColumnRenamed(
    "M48_Process_ID_Since_Last_Donation", ls_keycolname
)

ls_output_loc = ls_out_loc + "List-{0}".format(pl_list)
cl.write_2_file(logger, since_last_donation_df, ls_output_loc)
logger.info("Completed Since Last Donation List Processing")

# Creating table structure in RedShift
logger.info("Create SQL file for RedShift Table Structure")
ls_bucket_name = ls_out_loc.lstrip("s3://").split("/")[0]
ls_sql_loc = "/".join(ls_out_loc.lstrip("s3://").split("/")[1:]) + "sql/"
logger.info("List : {0}".format(int(pl_list)))

ls_target_tbl = lj_content.get("tblList-{0}".format(pl_list))
df_list = cl.fn_file_2_df(logger, spark, ls_output_loc)

ld_script = {
    "schema_name": "public",
    "table_name": ls_target_tbl,
    "bucket_name": ls_bucket_name,
    "output_path": ls_sql_loc,
    "data_location": ls_output_loc,
    "key_column": lj_content.get("KeyColumnName"),
    "key_col_type": lj_content.get("KeyColumnType", "VARCHAR(100)"),
    "column_prefix": str(lj_content.get("ColumnPrefix", "")).strip(" "),
    "column_suffix": str(lj_content.get("ColumnSuffix", "")).strip(" "),
}
cl.gen_ddl_script(logger, df_list.schema.fields, ld_script)
logger.info("SQL file for RedShift Table Structure created successfully")

logger.info("Since Last Donation List Processing completed successfully")
