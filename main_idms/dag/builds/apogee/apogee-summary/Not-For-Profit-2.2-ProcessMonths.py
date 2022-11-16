import logging
import sys
from pyspark.sql import SparkSession
import commonlib as cl
import pyspark.sql.functions as f
import json


def get_df_Months():
    global curDate
    lv_sql = "SELECT Cd_A, Cd_B, Cd_C, Cd_D, Cd_E, BDesc, CDesc, CFormula ,CatDesc, DFormula, " \
             "       replace(CFormula,'<MONTHS>',CatDesc) ColList, " \
             "        BDesc || '_' ||  CatDesc AS FieldName, Dom " \
             "  FROM (SELECT a.*, m.MName CatDesc " \
             "          FROM metadata a, Months m " \
             "         WHERE CDesc like '%<MONTHS>%') as Fnl "
    stg_tbl = spark.sql(lv_sql)
    stg_tbl.createOrReplaceTempView('stg_Meta_Mnth')

    v_gensql = "SELECT rn, ColList FROM ( " \
               "    SELECT  1 rn,        ColList                         , FieldName FName FROM stg_key_cols WHERE rn = 1 UNION " \
               "    SELECT  2 rn, ', '|| ColList || ' AS ' ||  FieldName , FieldName FName FROM stg_Meta_Mnth             UNION " \
               "    SELECT  3 rn,        ColList                         , FieldName FName FROM stg_key_cols WHERE rn = 3 and duration = 'M48' UNION " \
               "    SELECT  4 rn,        ColList                         , FieldName FName FROM stg_key_cols WHERE rn = 4 ) a" \
               " ORDER BY rn "
    ls_cols = spark.sql(v_gensql).select('ColList').collect()
    logger.info(v_gensql)

    v_gen_sql = cl.fn_gen_sql(ls_cols)
    v_gen_sql = v_gen_sql.replace('{ProcessDate}', curDate)
    logger.info("Generated Months SQL [{}]".format(v_gen_sql))
    return spark.sql(v_gen_sql)


# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName("MonthsCalculation").getOrCreate()
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
ls_outtmp = ls_temp_loc + "Months/"
logger.info("Location - Temp: {0}".format(ls_outtmp))

df_trans = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_Trans")
df_trans.createOrReplaceTempView("NP_Transactions")

# Read the Parquet file and continue as transaction input file. - End
next_step = "Dimension"

if next_step == "Dimension":
    la_cols = ["col.Code", "col.Value", "col.Formula"]
    dfA = cl.json_2_df(spark, ls_config + "PrefixA.json", la_cols)
    dfA.createOrReplaceTempView('LstA')
    logger.info("Prefix A Count: {}".format(dfA.count()))

    dfB = cl.json_2_df(spark, ls_config + "PrefixB.json", la_cols)
    dfB.createOrReplaceTempView('LstB')
    logger.info("Prefix B Count: {}".format(dfB.count()))

    dfC = cl.json_2_df(spark, ls_config + "PrefixC.json", la_cols)
    dfC.createOrReplaceTempView('LstC')
    logger.info("Prefix C Count: {}".format(dfC.count()))

    dfD = cl.json_2_df(spark, ls_config + "PrefixD.json", la_cols)
    dfD.createOrReplaceTempView('LstD')
    logger.info("Prefix D Count: {}".format(dfD.count()))

    la_cols = ["col.Code", "col.Value"]
    dfDom = cl.json_2_df(spark, ls_config + "Domain.json", la_cols)
    dfDom.createOrReplaceTempView('Domain')
    logger.info("DataType Count: {}".format(dfDom.count()))

    la_cols = ["col.A", "col.B", "col.C", "col.D", "col.E"]
    dfCombo = cl.json_2_df(spark, ls_config + "Combinations.json", la_cols)
    dfCombo.createOrReplaceTempView('fct')
    logger.info("Combinations Count: {}".format(dfCombo.count()))

    spark.sql("SELECT sequence(1, 12) as Records").select(f.explode("Records")) \
        .createOrReplaceTempView("Months1")

    lv_mnth_sql = "SELECT date_format(cast(concat('2100-', col, '-01') as DATE), 'MMMM') as MName from Months1"
    spark.sql(lv_mnth_sql).createOrReplaceTempView("Months")
    next_step = "MetaData"

if next_step == "MetaData":
    # Generate Metadata - Start
    vSQL = "SELECT a.Code Cd_A, a.Value ADesc, a.Formula AFormula, " \
           "       b.Code Cd_B, b.Value BDesc, b.Formula BFormula, " \
           "       c.Code Cd_C, c.Value CDesc, c.Formula CFormula, " \
           "       d.Code Cd_D, d.Value DDesc, d.Formula DFormula, " \
           "       e.Code Cd_E, e.Value Dom " \
           "  FROM fct, LstA a, LstB b, LstC c, LstD d, Domain e " \
           " WHERE a.Code = fct.A and b.Code = fct.B and c.Code = fct.C " \
           "   AND d.Code = fct.D and e.Code = fct.E "
    spark.sql(vSQL).createOrReplaceTempView("metadata")
    cl.showdata(logger, spark, "metadata",100)
    # Generate Metadata - End

    # Create Key and Group BY Column - Start [Table: stg_key_cols]
    lv_sql_key = " SELECT 1 as rn,   'ALL' as duration, 'SELECT ' || DDesc  || ' AS ' || DDesc ColList, DDesc as FieldName FROM metadata a where DFormula = 'ID'" \
                 "  UNION ALL " \
                 " SELECT 3 as rn,   'ALL' as duration, ' FROM  NP_Transactions', 'From' FieldName " \
                 "  UNION ALL " \
                 " SELECT 3.1 as rn, 'ALL' as duration, ' FROM  NP_TransCat',     'Cat' FieldName " \
                 "  UNION ALL " \
                 " SELECT 3.2 as rn, 'ALL' as duration, ' FROM  NP_TransList',    'List' FieldName " \
                 "  UNION ALL " \
                 " SELECT 4 as rn,   'ALL' as duration, ' GROUP BY ' || DDesc, DDesc as FieldName  FROM metadata a where DFormula = 'ID'"

    for rw in dfA.filter("length(Formula) > 0").collect():
        lv_sql_key = lv_sql_key + " UNION ALL SELECT 3 as rn,  '{0}' as duration, ' " \
                                  "  FROM NP_Transactions WHERE {1}', " \
                                  " 'From' FieldName ".format(rw['Value'], rw['Formula'])

    spark.sql(lv_sql_key).createOrReplaceTempView("stg_key_cols")
    cl.showdata(logger, spark, "stg_key_cols")
    # Create Key and Group BY Column - End
    next_step = "ProcessMonths"
    # Data Generated for the columns for MONTHS

if next_step == "ProcessMonths":
    # Start Generate Query for Month Based calculation
    logger.info("Starting Months Calculation ")
    lv_months_sql = "select MName as ColList from Months"
    ll_months = cl.fn_gen_sql(spark.sql(lv_months_sql).select('ColList').collect()) \
        .lstrip().split(" ")
    ls_tblname = "stg_cat_Months_tbl"
    ls_keycolname = lj_content.get("KeyColumnName")
    df_mon_tbl = get_df_Months().withColumnRenamed("Process_ID", ls_keycolname)

    # Generate Final table for LTD / M12 / M24 / M48 - Start
    ls_output_loc = ls_out_loc + "Months"
    cl.write_2_file(logger, df_mon_tbl, ls_output_loc)

    # Creating table structure in RedShift
    logger.info("Create SQL file for RedShift Table Structure")
    ls_bucket_name = ls_out_loc.lstrip('s3://').split('/')[0]
    ls_sql_loc = '/'.join(ls_out_loc.lstrip('s3://').split('/')[1:]) + "sql/"

    ls_target_tbl = lj_content.get("tblMonths")
    df_mon_tbl = cl.fn_file_2_df(logger, spark, ls_output_loc)

    ld_script = {"schema_name": "public",
                 "table_name": ls_target_tbl,
                 "bucket_name": ls_bucket_name,
                 "output_path": ls_sql_loc,
                 "data_location": ls_output_loc,
                 "key_column": lj_content.get("KeyColumnName"),
                 "key_col_type": lj_content.get("KeyColumnType", "VARCHAR(100)"),
                 "column_prefix": str(lj_content.get("ColumnPrefix", "")).strip(' '),
                 "column_suffix": str(lj_content.get("ColumnSuffix", "")).strip(' ')
                 }
    cl.gen_ddl_script(logger, df_mon_tbl.schema.fields, ld_script)
    # cl.gen_ddl_script(logger, df_mon_tbl.schema.fields, "public", ls_target_tbl, ls_bucket_name,
    #                   ls_sql_loc, ls_output_loc, ls_iam_role)
    logger.info("SQL file for RedShift Table Structure created successfully")

logger.info("Months Calculation completed successfully")
