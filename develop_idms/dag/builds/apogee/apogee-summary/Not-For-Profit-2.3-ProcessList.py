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


def getListData(ps_duration, ps_filter="AVG"):
    logger.info("getListData Filter: {0}".format(ps_filter))
    lv_sql_ltd = "SELECT Cd_A, Cd_B, Cd_C, Cd_D, Cd_E, CatDesc, AFormula, BFormula, " \
                 "       replace(DFormula,'<B_FORMULA>',NVL2(BFormula,BFormula,'')) DFormula1,  " \
                 "       replace(replace(replace(DFormula,'<C_FORMULA>',cFormula),'<A_FORMULA>',AFormula)," \
                 "               '<LISTID>', char(39) || CatDesc || char(39) ) DFormula, " \
                 "       regexp_replace(replace(replace( concat(ADesc, ASep, BDesc, BSep, CatDesc, CSep, DDesc) " \
                 "               ,'LTD_Lowest_Donation_Amount', 'LTD_Lowest_Donation_Amount_Ever') " \
                 "       , 'LTD_Highest_Donation_Amount', 'LTD_Highest_Donation_Amount_Ever'), '_+','_') " \
                 "       AS FieldName, Dom " \
                 "  FROM (SELECT a.*, " \
                 "               CASE WHEN LENGTH(ADesc) > 0 THEN '_' ELSE '' END ASep, " \
                 "               CASE WHEN LENGTH(bDesc) > 0 THEN '_' ELSE '' END BSep, " \
                 "               CASE WHEN LENGTH(c.Code)> 0 THEN '_' ELSE '' END CSep," \
                 "               c.Code CatDesc " \
                 "          FROM metadata a, List c " \
                 "         WHERE CDesc LIKE '%<LISTID>%' " \
                 "           AND DDesc = '{0}'" \
                 "           AND ADesc = '{1}') as Fnl ".format(ps_filter, ps_duration)
    stg_tbl = spark.sql(lv_sql_ltd)
    logger.info(lv_sql_ltd)
    cl.write_2_file(logger, stg_tbl, ls_outtmp + "stg_tbl")
    stg_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "stg_tbl")
    stg_tbl.createOrReplaceTempView('stg_List_Meta')
    cl.showdata(logger, spark, "stg_List_Meta")


def get_df_List(ps_duration, ps_filter):
    global curDate
    global ls_temp_loc

    lv_sql_ltd = "SELECT * FROM stg_List_Meta"

    stg_tbl = spark.sql(lv_sql_ltd)
    if stg_tbl.count() == 0:
        return None
    cl.write_2_file(logger, stg_tbl, ls_outtmp + "stg_tbl_list_" + ps_filter)
    stg_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "stg_tbl_list_" + ps_filter)
    stg_tbl.createOrReplaceTempView('stg_Meta_List')

    v_gensql = "SELECT rn, ColList FROM ( " \
               "    SELECT  1 rn,                       ColList, FieldName FName FROM stg_key_cols " \
               "     WHERE rn = 1 " \
               "     UNION " \
               "    SELECT  2 rn, ', ' || DFormula  || ' AS ' || FieldName ColList, FieldName FName FROM stg_Meta_List " \
               "     WHERE trim(DFormula) IS NOT NULL AND DFormula != 'ID'" \
               "     UNION " \
               "    SELECT  3 rn,                       ColList, FieldName FName FROM stg_key_cols " \
               "     WHERE rn = 3 and duration = '{1}'" \
               "     UNION " \
               "    SELECT  3.1 rn, ' AND length(ListID) > 0 ' ColList, 'Where'   FName" \
               "     UNION " \
               "    SELECT  4 rn,                       ColList, FieldName FName FROM stg_key_cols WHERE rn = 4 ) a" \
               " ORDER BY rn, FName ".format(ps_filter, ps_duration)

    ls_ColList = spark.sql(v_gensql).sort('rn', 'ColList').select('ColList').collect()
    v_gen_sql = cl.fn_gen_sql(ls_ColList)
    v_gen_sql = v_gen_sql.replace('{ProcessDate}', curDate)
    logger.info("Generated List SQL [{0}]".format(v_gen_sql))
    return spark.sql(v_gen_sql)


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s: %(message)s')

# Setup Variables.
logger.info("Setting up environment variables")
ls_content = str(sys.argv[1]).replace("'", '"')
lj_content = json.loads(ls_content)

pd_process_date = sys.argv[2]
pl_list = sys.argv[3]

# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName("ListCalculation-{0}".format(pl_list)).getOrCreate()
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

df_trans = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_Trans")
df_trans.createOrReplaceTempView("NP_Transactions")

# List Base Data
df_list = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_TransList")
df_list.createOrReplaceTempView("NP_TransList")

# Read the Parquet file and continue as transaction input file. - End
next_step = "Dimension"

if next_step == "Dimension":
    # Get List ID's from Transaction file
    dfList = df_list.select("ListID").groupby("ListID").count().withColumnRenamed("ListID", "Code")
    dfList.createOrReplaceTempView('List')
    logger.info("ListID Count: {}".format(dfList.count()))

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
    cl.showdata(logger, spark, "metadata")
    # Generate Metadata - End

    # Create Key and Group BY Column - Start [Table: stg_key_cols]
    lv_sql_key = " SELECT 1 as rn, 'ALL' as duration, 'SELECT ' || DDesc  || ' AS ' || DDesc ColList, DDesc as FieldName FROM metadata a where DFormula = 'ID'" \
                 "  UNION ALL " \
                 " SELECT 3 as rn, 'ALL' as duration, ' FROM  NP_TransList', 'From' FieldName " \
                 "  UNION ALL " \
                 " SELECT 4 as rn, 'ALL' as duration, ' GROUP BY ' || DDesc, DDesc as FieldName  FROM metadata a where DFormula = 'ID'"
    for rw in dfA.filter("length(Formula) > 0").collect():
        lv_sql_key = lv_sql_key + " UNION ALL SELECT 3 as rn,  '{0}' as duration, " \
                                  "    ' FROM NP_TransList WHERE {1} ', " \
                                  "    'From' FieldName ".format(rw['Value'], rw['Formula'])

    spark.sql(lv_sql_key).createOrReplaceTempView("stg_key_cols")
    cl.showdata(logger, spark, "stg_key_cols")
    # Create Key and Group BY Column - End
    next_step = "ProcessList"

if next_step == "ProcessList":
    # Start Generate Query for the columns that have CATEGORY
    logger.info("Start Processing Lists")
    # Get the types of processing for Lists and convert it as an array [ AVG / COUNT / etc ]
    lv_list_sql = "select distinct DDesc ColList FROM metadata where CDesc like '%<LISTID>%' order by 1"
    ll_lists = cl.fn_gen_sql(spark.sql(lv_list_sql).select('ColList').collect()).lstrip().split(" ")
    la_tbllist = []
    if len(ll_lists) - 1 < int(pl_list):
        logger.info("No Lists Calculation to perform. Exiting")
        exit(0)

    curListType = ll_lists[int(pl_list)]

    lv_list_sql = "select distinct ADesc ColList from metadata where CDesc like '%<LISTID>%' "
    ll_cols_list = (
        cl.fn_gen_sql(spark.sql(lv_list_sql).select('ColList').collect())).lstrip().split(" ")
    for rec in ll_cols_list:
        # Process the specific list type [AVERAGE/COUNT/etc.,]
        getListData(rec, curListType)
        ls_keycolname = "list_" + rec + "_" + pl_list + "_Process_ID"
        ls_tblname = "stg_list_" + rec + "_" + pl_list + "_tbl"
        logging.info("Starting {0} ".format(ls_tblname))
        df_list_tbl = get_df_List(rec, pl_list)
        if df_list_tbl != None:
            df_list_tbl = df_list_tbl.withColumnRenamed("Process_ID", ls_keycolname)
            la_tbllist.append(ls_tblname + "." + ls_keycolname)
            cl.write_2_file(logger, df_list_tbl, ls_outtmp + ls_tblname)
            df_list_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + ls_tblname)
            df_list_tbl.createOrReplaceTempView(ls_tblname)
            logger.info("Completing {0} ".format(ls_tblname))
        else:
            logger.info("Skip Dataset {0} ".format(ls_tblname))

    # Generate Final table for LTD / M12 / M24 / 48 - Start
    ltbl = la_tbllist[0].split('.')
    ls_FTbl = ltbl[0]
    ls_FCol = ltbl[1]
    df_oj_tbl = spark.sql("SELECT * FROM " + ls_FTbl)
    #  Outer Join all Category Staging tables - Start
    for curList in la_tbllist[1:]:
        # ls_From = " FROM " + ls_FTbl
        ls_cur_col = curList.split('.')[1]
        ls_cmp_id = "COALESCE( " + ls_FCol + ", " + ls_cur_col + ") as Process_ID, "
        ls_cur_tbl = curList.split('.')[0]
        # ls_From = " FROM " + ls_FTbl + " FULL OUTER JOIN " + ls_cur_tbl + " ON " + ls_FTbl
        # + "." + ls_FCol + " = " + ls_cur_tbl + "." + ls_cur_col
        ls_From = " FROM {0} FULL OUTER JOIN {2} ON {0}.{1} = {2}.{3}" \
            .format(ls_FTbl, ls_FCol, ls_cur_tbl, ls_cur_col)
        ls_final_sql = "SELECT " + ls_cmp_id + " * " + ls_From + " ORDER BY COALESCE( " + ls_FCol + ", " + ls_cur_col + ") "
        logger.info(ls_final_sql)
        df_oj_tbl = spark.sql(ls_final_sql)
        df_oj_tbl = df_oj_tbl.withColumnRenamed(ls_FCol, "ToDrop") \
            .withColumnRenamed("Process_ID", ls_FCol).drop("ToDrop").drop(ls_cur_col)
        cl.write_2_file(logger, df_oj_tbl, ls_outtmp + "OuterJoin_" + ls_cur_tbl)
        df_oj_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "OuterJoin_" + ls_cur_tbl)
        df_oj_tbl.createOrReplaceTempView(ls_FTbl)
    #  Outer Join all Category Staging tables - End

    ls_keycolname = lj_content.get("KeyColumnName")
    # df_oj_tbl = df_oj_tbl.withColumnRenamed(ls_FCol, "List_{0}_Process_ID".format(pl_list))
    df_oj_tbl = df_oj_tbl.withColumnRenamed(ls_FCol, ls_keycolname)
    ls_output_loc = ls_out_loc + "List-{0}".format(pl_list)
    cl.write_2_file(logger, df_oj_tbl, ls_output_loc)
    logger.info("Completing List Processing")

    # Creating table structure in RedShift
    logger.info("Create SQL file for RedShift Table Structure")
    ls_bucket_name = ls_out_loc.lstrip('s3://').split('/')[0]
    ls_sql_loc = '/'.join(ls_out_loc.lstrip('s3://').split('/')[1:]) + "sql/"
    logger.info("List : {0}".format(int(pl_list)))

    ls_target_tbl = lj_content.get("tblList-{0}".format(pl_list))
    df_list = cl.fn_file_2_df(logger, spark, ls_output_loc)

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
    cl.gen_ddl_script(logger, df_list.schema.fields, ld_script)
    # cl.gen_ddl_script(logger, df_list.schema.fields, "public", ls_target_tbl, ls_bucket_name,
    #                   ls_sql_loc, ls_output_loc, ls_iam_role)
    logger.info("SQL file for RedShift Table Structure created successfully")

logger.info("List Processing completed successfully")
