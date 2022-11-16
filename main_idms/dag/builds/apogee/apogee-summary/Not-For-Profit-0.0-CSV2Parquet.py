import json
import logging
import sys

from pyspark.sql import SparkSession

import commonlib as cl

# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName("CSV2Parquet").getOrCreate()
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

ls_app_name = lj_content.get("app_name")
ls_base = lj_content.get("base_location")
ls_code = lj_content.get("code_base")

logger.info("Starting Read Input Process")
ls_prcs_loc = ls_base + lj_content.get("process_location")
ls_rej_loc = ls_base + lj_content.get("reject_location")
ls_dsc_loc = ls_base + lj_content.get("discard_location")
ls_config = ls_code + lj_content.get("config_location") + ls_app_name + "/"
# ls_input_file = lj_content.get("input_file_base") + lj_content.get("input_file_name")
# ls_input_file = lj_content.get("input_file_name")
ls_input_file = json.loads(str(lj_content.get("input_file_name")).replace("~", '"'))
ls_input_json = ls_config + "input_layout.json"

# Create Parquet file from CSV - Start
logger.info("Starting CSV 2 Parquet process")
ldf_output = cl.fn_file_2_df(logger, spark, ls_input_file, ls_input_json, "csv", ls_rej_loc)
ldf_output = cl.fn_discard_data(logger, spark, ldf_output, ls_input_json, ls_dsc_loc,
                                pd_process_date)
cl.write_2_file(logger, ldf_output, ls_prcs_loc)
logger.info("CSV 2 Parquet process completed successfully")
