import json
import logging
import sys

from pyspark.sql import SparkSession

import commonlib as cl


def get_df_noCategory(ps_filter):
    global curDate
    lv_sql_ltd = "SELECT Cd_A, Cd_B, Cd_C, Cd_D, Cd_E, CatDesc, AFormula, BFormula, " \
                 "      replace(replace(replace(DFormula,'<B_FORMULA>',NVL2(BFormula,BFormula,'')) " \
                 "                      ,'<C_FORMULA>',NVL2(CFormula,CFormula,' 1 = 1 '))" \
                 "             , '<A_FORMULA>',NVL2(AFormula,AFormula,' 1 = 1 ')) DFormula,  " \
                 "      regexp_replace(replace(replace( concat(ADesc, ASep, BDesc,  BSep, CatDesc, DDesc) " \
                 "               ,'LTD_Lowest_Donation_Amount', 'LTD_Lowest_Donation_Amount_Ever') " \
                 "      , 'LTD_Highest_Donation_Amount', 'LTD_Highest_Donation_Amount_Ever'), '_+','_')     " \
                 "       AS FieldName, Dom " \
                 "  FROM (SELECT a.*, " \
                 "               CASE WHEN LENGTH(ADesc) > 0 THEN '_' ELSE '' END ASep, " \
                 "               CASE WHEN LENGTH(bDesc) > 0 THEN '_' ELSE '' END BSep, " \
                 "               '' CatDesc " \
                 "          FROM metadata a " \
                 "         WHERE CDesc NOT like '%<%>%') as Fnl "
    stg_tbl = spark.sql(lv_sql_ltd)
    logger.info(lv_sql_ltd)
    stg_tbl.createOrReplaceTempView('stg_meta_ltd')
    cl.showdata(logger, spark, "stg_meta_ltd", 100)

    v_gensql = "SELECT rn, ColList FROM ( " \
               "    SELECT  1 rn, ColList, FieldName FName FROM stg_key_cols WHERE rn = 1 UNION " \
               "    SELECT  2 rn, ', ' || DFormula  || ' AS ' || FieldName   ColList, FieldName FName " \
               "      FROM stg_meta_ltd WHERE trim(DFormula) IS NOT NULL AND DFormula != 'ID' and FieldName like '{0}%' UNION " \
               "    SELECT  3 rn, ColList, FieldName FName FROM stg_key_cols WHERE rn = 3 and duration = '{1}' UNION " \
               "    SELECT  4 rn, ColList, FieldName FName FROM stg_key_cols WHERE rn = 4 ) a" \
               " ORDER BY rn, FName ".format(ps_filter, ps_filter)
    logger.info("Gen SQL: {}".format(v_gensql))
    ls_ColList = spark.sql(v_gensql).select('ColList').collect()
    v_gen_sql = cl.fn_gen_sql(ls_ColList)
    v_gen_sql = v_gen_sql.replace('{ProcessDate}', curDate)
    logger.info("Generated [{0}] SQL [{1}]".format(ps_filter, v_gen_sql))
    return spark.sql(v_gen_sql)


def generate_query(ps_key_col, ps_categoryCols="ListCategory01"):
    lv_sql = " SELECT 'SELECT Process_ID as {0} ' as ColList " \
             " UNION ALL  " \
             " SELECT ',' || RwCondn || ' as ' || ColList  " \
             "   FROM (SELECT ' PriCat_' || Code as ColList, " \
             "                ' MAX(CASE WHEN ' || char(39) || Code|| char(39) ||' IN ({1})  THEN 1 ELSE 0 END) ' RwCondn," \
             "                 Code   " \
             "           FROM Category) b " \
             " UNION ALL " \
             " SELECT ' FROM  NP_TransCat' " \
             " UNION ALL " \
             " SELECT ' GROUP BY Process_ID ' ".format(ps_key_col, ps_categoryCols)
    return spark.sql(lv_sql)


# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName("LTDCalculation").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
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
ls_catcols = lj_content.get("CatColumnList", "ListCategory01")
ls_app_name = lj_content.get("app_name")
ls_config = ls_code + lj_content.get("config_location") + ls_app_name + "/"

logger.info("Starting Read Input Process")
ls_prcs_loc = ls_base + lj_content.get("process_location")
ls_rej_loc = ls_base + lj_content.get("reject_location")
ls_dsc_loc = ls_base + lj_content.get("discard_location")
ls_temp_loc = ls_base + lj_content.get("temp_location")
ls_out_loc = ls_base + lj_content.get("output_location")

ls_outtmp = ls_temp_loc + "LTD/"

df_trans = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_Trans")
df_trans.createOrReplaceTempView("NP_Transactions")

# Category Base Data
df_catgy = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_TransCat")
df_catgy.createOrReplaceTempView("NP_TransCat")

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
    cl.showdata(logger, spark, "metadata", 100)
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
        lv_sql_key = lv_sql_key + " UNION ALL SELECT 3 as rn,  '{0}' as duration, " \
                                  "    ' FROM NP_Transactions WHERE {1} ', " \
                                  "    'From' FieldName ".format(rw['Value'], rw['Formula'])

    spark.sql(lv_sql_key).createOrReplaceTempView("stg_key_cols")
    cl.showdata(logger, spark, "stg_key_cols", 100)
    # Create Key and Group BY Column - End
    next_step = "ProcessPriCat"
    # Data Generated for the columns that does not have CATEGORY / LIST / MONTHS [ Only LTD Values ]

if next_step == "ProcessPriCat":
    # Get Category ID's from Transaction file
    v_SQL = ""
    #  Distinct is used here to pick unique records if there is only one column used for category
    # Start Changes - IDMS-2172 Pricat rollups are only calculating with ListCategory01
    pricat_catcols = str(ls_catcols).split(',')[0]
    v_SQL = v_SQL + " UNION SELECT DISTINCT {0} FROM NP_TransCat".format(pricat_catcols.strip())
    # ENd Changes - IDMS-2172 Pricat rollups are only calculating with ListCategory01

    df_catgy = spark.sql(v_SQL.strip(" UNION")) \
        .withColumnRenamed(pricat_catcols, "Code") \
        .filter("length(Code) > 0")
    df_catgy.createOrReplaceTempView('Category')
    logger.info("Category Count: {}".format(df_catgy.count()))
    cl.showdata(logger, spark, "Category", 50)

    df_query = cl.fn_gen_sql(
        generate_query("PriCat_Process_ID", pricat_catcols).select('ColList').collect())
    df_pricat = spark.sql(df_query)
    cl.write_2_file(logger, df_pricat, ls_outtmp + "PriCat_Info")
    df_oj_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "PriCat_Info")
    df_oj_tbl.createOrReplaceTempView('PriCat_Info')
    cl.showdata(logger, spark, "PriCat_Info", 20)
    next_step = "ProcessLTD"

if next_step == "ProcessLTD":
    # Generate Query for the columns that does not have CATEGORY / LIST / MONTHS [Only LTD]
    lv_list_sql = "select Value as ColList from LstA where length(Value)> 0"
    ll_cols_list = (
        cl.fn_gen_sql(spark.sql(lv_list_sql).select('ColList').collect())).lstrip().split(" ")
    for rec in ll_cols_list:
        ls_dataset = rec.replace("_", "")
        ls_tblname = "stg_" + ls_dataset + "_tbl"
        logger.info("Starting {0} ".format(ls_dataset))
        df_nocat_tbl = get_df_noCategory(ls_dataset).withColumnRenamed("Process_ID",
                                                                       rec + "_Process_ID")
        # Write it to parquet file to break the DAG Starts Here
        cl.write_2_file(logger, df_nocat_tbl, ls_outtmp + ls_tblname)
        df_nocat_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + ls_tblname)
        # Write it to parquet file to break the DAG Ends Here

        df_nocat_tbl.createOrReplaceTempView(ls_tblname)
        logger.info("Completing {0} ".format(ls_dataset))

    #  Outer Join all Category Staging tables - Start
    ls_FTbl = "stg_" + ll_cols_list[0] + "_tbl"
    ls_FCol = ll_cols_list[0] + "_Process_ID"
    for rec in ll_cols_list[1:]:
        df_list_tbl = cl.gen_outer_join(logger, spark, ls_FTbl, ls_FCol, "stg_" + rec + "_tbl",
                                        rec + "_Process_ID")
        cl.write_2_file(logger, df_list_tbl, ls_outtmp + "Phase1_" + rec)
        df_list_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "Phase1_" + rec)
        df_list_tbl.createOrReplaceTempView(ls_FTbl)
    #  Outer Join all Category Staging tables - End

    # # Perform another outer join to combine PriCat info
    df_list_tbl = cl.gen_outer_join(logger, spark, ls_FTbl, ls_FCol, "PriCat_Info",
                                    "PriCat_Process_ID")
    cl.write_2_file(logger, df_list_tbl, ls_outtmp + "Phase1_InfoOj")
    df_list_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "Phase1_InfoOj")
    cl.showdata(logger, spark, df_list_tbl)

    # Perform another outer join to combine PriCat info

    # Generate Final table for LTD / M12 / M24 / M48 - Start
    ls_output_loc = ls_out_loc + "Phase1"
    df_list_tbl = df_list_tbl.withColumnRenamed(ls_FCol, lj_content.get("KeyColumnName"))
    cl.write_2_file(logger, df_list_tbl, ls_output_loc)

    # Creating table structure in RedShift
    logger.info("Create SQL file for RedShift Table Structure")
    ls_bucket_name = ls_out_loc.lstrip('s3://').split('/')[0]
    ls_sql_loc = '/'.join(ls_out_loc.lstrip('s3://').split('/')[1:]) + "sql/"

    ls_target_tbl = lj_content.get("tblLTD")

    df_ltd = cl.fn_file_2_df(logger, spark, ls_output_loc)
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
    cl.gen_ddl_script(logger, df_ltd.schema.fields, ld_script)

    logger.info("SQL file for RedShift Table Structure created successfully")

logger.info("LTD calculation completed successfully")
