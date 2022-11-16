import logging
import sys
from pyspark.sql import SparkSession
import commonlib as cl
import json


def getCategoryData():
    lv_sql_ltd = "SELECT Cd_A, Cd_B, Cd_C, Cd_D, Cd_E, CatDesc, AFormula, BFormula," \
                 "       replace(DFormula,'<B_FORMULA>',NVL2(BFormula,BFormula,'')) DFormula1,  " \
                 "       replace(replace(replace(DFormula,'<C_FORMULA>',cFormula),'<A_FORMULA>',AFormula)," \
                 "               '<CATEGORY>', char(39) || CatDesc || char(39) ) DFormula, " \
                 "      regexp_replace(replace(replace( concat(ADesc, ASep, BDesc, BSep, CatDesc, CSep, DDesc) " \
                 "               ,'LTD_Lowest_Donation_Amount', 'LTD_Lowest_Donation_Amount_Ever') " \
                 "      , 'LTD_Highest_Donation_Amount', 'LTD_Highest_Donation_Amount_Ever'),'_+','_') " \
                 "       AS FieldName, Dom " \
                 "  FROM (SELECT a.*, " \
                 "               CASE WHEN LENGTH(ADesc) > 0 THEN '_' ELSE '' END ASep, " \
                 "               CASE WHEN LENGTH(bDesc) > 0 THEN '_' ELSE '' END BSep, " \
                 "               CASE WHEN LENGTH(c.Code)> 0 THEN '_' ELSE '' END CSep," \
                 "               c.Code CatDesc " \
                 "          FROM metadata a, Category c " \
                 "         WHERE CDesc like '%<CATEGORY>%') as Fnl "
    logger.info(lv_sql_ltd)
    stg_List_Meta = spark.sql(lv_sql_ltd)
    cl.write_2_file(logger, stg_List_Meta, ls_outtmp + "stg_tbl")
    stg_List_Meta = cl.fn_file_2_df(logger, spark, ls_outtmp + "stg_tbl")
    stg_List_Meta.createOrReplaceTempView('stg_Category_Meta')
    cl.showdata(logger, spark, "stg_Category_Meta")


def get_df_Category(ps_filter, ps_category, ps_categoryCols="ListCategory01"):
    global curDate
    global ls_temp_loc

    ls_filter = ps_filter[1:3]
    if ls_filter == 'TD' or ps_filter == 'ALL':
        ls_filter = 1000

    lv_sql_ltd = "SELECT * FROM stg_Category_Meta " \
                 " WHERE CatDesc = '{0}' " \
                 "   AND FieldName like '{1}%' ".format(ps_category, ps_filter)
    stg_tbl = spark.sql(lv_sql_ltd)
    if stg_tbl.count() == 0:
        return None

    cl.write_2_file(logger, stg_tbl, ls_outtmp + "stg_tbl_" + ps_category)
    stg_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "stg_tbl_" + ps_category)
    stg_tbl.createOrReplaceTempView('stg_Meta_Cat')

    v_gensql = "SELECT rn, ColList FROM ( " \
               "    SELECT  1 rn, ColList, FieldName FName FROM stg_key_cols WHERE rn = 1 UNION " \
               "    SELECT  2 rn, ', ' || DFormula  || ' AS ' || FieldName   ColList, FieldName FName " \
               "      FROM stg_Meta_Cat WHERE trim(DFormula) IS NOT NULL AND DFormula != 'ID' and FieldName like '{0}%' UNION " \
               "    SELECT  3 rn, ColList, FieldName FName FROM stg_key_cols WHERE rn = 3.1 and duration = 'ALL' UNION " \
               "    SELECT  3.1 rn, ' WHERE MB_DonationDate <= {1} AND ' || chr(39) || {3}{2}{3} || chr(39) || ' in ({4}) ' ColList, 'Where' FName UNION " \
               "    SELECT  4 rn, ColList, FieldName FName FROM stg_key_cols WHERE rn = 4 ) a" \
               " ORDER BY rn, FName ".format(ps_filter, ls_filter, ps_category, chr(39),
                                             ps_categoryCols)

    logger.info(v_gensql)
    ls_ColList = spark.sql(v_gensql).sort('rn', 'ColList').select('ColList').collect()
    v_gen_sql = cl.fn_gen_sql(ls_ColList)
    v_gen_sql = v_gen_sql.replace('{ProcessDate}', curDate)
    logger.info("Generated Category SQL [{}]".format(v_gen_sql))
    return spark.sql(v_gen_sql)


# Setup Spark Context,  Session Builder and Loggers
spark = SparkSession.builder.appName("CategoryCalculation").getOrCreate()
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
ls_catcols = lj_content.get("CatColumnList", "ListCategory01")
ls_app_name = lj_content.get("app_name")
ls_config = ls_code + lj_content.get("config_location") + ls_app_name + "/"

logger.info("Starting Read Input Process")
ls_prcs_loc = ls_base + lj_content.get("process_location")
ls_rej_loc = ls_base + lj_content.get("reject_location")
ls_dsc_loc = ls_base + lj_content.get("discard_location")
ls_temp_loc = ls_base + lj_content.get("temp_location")
ls_out_loc = ls_base + lj_content.get("output_location")
ls_outtmp = ls_temp_loc + "Category/"
logger.info("Location - Temp: {0}".format(ls_outtmp))

df_trans = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_Trans")
df_trans.createOrReplaceTempView("NP_Transactions")

# Category Base Data
df_catgy = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_TransCat")
df_catgy.createOrReplaceTempView("NP_TransCat")

# List Base Data
df_list = cl.fn_file_2_df(logger, spark, ls_temp_loc + "NP_TransList")
df_list.createOrReplaceTempView("NP_TransList")
cl.showdata(logger, spark, df_list)
# Read the Parquet file and continue as transaction input file. - End
next_step = "Dimension"

if next_step == "Dimension":
    v_SQL = ""
    for rec in str(ls_catcols).split(','):
        v_SQL = v_SQL + " UNION SELECT DISTINCT {0} FROM NP_TransCat".format(rec.strip())
    dfCat = spark.sql(v_SQL.strip(" UNION")) \
        .withColumnRenamed(str(ls_catcols).split(',')[0], "Code")\
        .filter("length(Code) > 0")
    dfCat.createOrReplaceTempView('Category')

    if dfCat.count() <= 0:
        logger.error("No Category Data Found!!!! ")
        exit(1)
    logger.info("Category Count: {}".format(dfCat.count()))

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
    next_step = "ProcessCategory"
    # Data Generated for the columns that have CATEGORY

if next_step == "ProcessCategory":
    # Start Generate Query for the columns that have CATEGORY
    logger.info("Start Processing Category ")
    lv_sql = "SELECT Category.Code || '-' || LstA.Value as ColList " \
             "  FROM LstA, Category  " \
             " WHERE length(Value)> 0 AND length(Category.Code) > 0 "
    logger.info(lv_sql)
    ll_category = sorted(
        (cl.fn_gen_sql(spark.sql(lv_sql).select('ColList').collect())).lstrip().split(" "))
    getCategoryData()
    la_tbllist = []
    for curCat in ll_category:
        la_list = curCat.split('-')
        ls_listcode = la_list[0]
        ls_filter = la_list[1]
        ls_dataset = ls_filter.replace("_", "")
        ls_keycolname = ls_filter + "_" + ls_listcode + "_Process_ID"
        ls_tblname = "stg_cat_" + ls_filter + "_" + ls_listcode + "_tbl"
        logger.info("Starting {0} ".format(ls_tblname))
        df_category_tbl = get_df_Category(ls_dataset, ls_listcode, ls_catcols)
        if df_category_tbl is not None:
            df_category_tbl = df_category_tbl.withColumnRenamed("Process_ID", ls_keycolname)
            la_tbllist.append(ls_tblname + "." + ls_keycolname)
            cl.write_2_file(logger, df_category_tbl, ls_outtmp + ls_tblname)
            df_category_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + ls_tblname)
            df_category_tbl.createOrReplaceTempView(ls_tblname)
        else:
            logger.info("Skip creating {0} ".format(ls_tblname))

    # Generate Final table for LTD / M12 / M24 / M48 - Start

    ltbl = la_tbllist[0].split('.')
    ls_FTbl = ltbl[0]
    ls_FCol = ltbl[1]

    #  Outer Join all Category Staging tables - Start
    for curCat in la_tbllist[1:]:
        ls_cur_col = curCat.split('.')[1]
        ls_cmp_id = "COALESCE( " + ls_FCol + ", " + ls_cur_col + ") as Process_ID, "
        ls_cur_tbl = curCat.split('.')[0]
        ls_From = " FROM {0} FULL OUTER JOIN {2} ON {0}.{1} = {2}.{3}" \
            .format(ls_FTbl, ls_FCol, ls_cur_tbl, ls_cur_col)
        ls_order_by = " ORDER BY COALESCE({0}, {1}) ".format(ls_FCol, ls_cur_col)
        ls_final_sql = "SELECT " + ls_cmp_id + " * " + ls_From + ls_order_by
        logger.info(ls_final_sql)
        df_oj_tbl = spark.sql(ls_final_sql)
        df_oj_tbl = df_oj_tbl.withColumnRenamed(ls_FCol, "ToDrop") \
            .withColumnRenamed("Process_ID", ls_FCol).drop("ToDrop").drop(ls_cur_col)
        cl.write_2_file(logger, df_oj_tbl, ls_outtmp + "OuterJoin_" + ls_cur_tbl)
        df_oj_tbl = cl.fn_file_2_df(logger, spark, ls_outtmp + "OuterJoin_" + ls_cur_tbl)
        df_oj_tbl.createOrReplaceTempView(ls_FTbl)
    #  Outer Join all Category Staging tables - End

    df_oj_tbl = df_oj_tbl.withColumnRenamed(ls_FCol, lj_content.get("KeyColumnName"))

    ls_output_loc = ls_out_loc + "Category"
    cl.write_2_file(logger, df_oj_tbl, ls_output_loc)
    logger.info("Completing Category Processing")

    # Creating table structure in RedShift
    logger.info("Create SQL file for RedShift Table Structure")
    ls_bucket_name = ls_out_loc.lstrip('s3://').split('/')[0]
    ls_sql_loc = '/'.join(ls_out_loc.lstrip('s3://').split('/')[1:]) + "sql/"
    ls_target_tbl = lj_content.get("tblCategory")

    df_Category = cl.fn_file_2_df(logger, spark, ls_output_loc)
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
    # cl.gen_ddl_script(logger, df_Category.schema.fields, "public", ls_target_tbl, ls_bucket_name,
    #                   ls_sql_loc, ls_output_loc, ls_iam_role)
    cl.gen_ddl_script(logger, df_Category.schema.fields, ld_script)
    logger.info("SQL file for RedShift Table Structure created successfully")

    # Cleanup Temp Space - Start
    for rec in la_tbllist:
        ls_cur_tbl = rec.split('.')[0]
        df_category_tbl = spark.sql("SELECT current_date() curdate")
        cl.write_2_file(logger, df_category_tbl, ls_outtmp + "OuterJoin_" + ls_cur_tbl)
        cl.write_2_file(logger, df_category_tbl, ls_outtmp + ls_cur_tbl)
    # Cleanup Temp Space - End
logger.info("Category calculation completed successfully")
