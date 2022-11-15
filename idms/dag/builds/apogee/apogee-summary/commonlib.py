import json
from datetime import datetime

import boto3
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, FloatType, LongType, ShortType, StringType, DateType
from pyspark.sql.types import StructType, StructField


# Create DDL script from DataFrame on s3 location to create the table in Redshift
def gen_ddl_script(pl_logger, pd_dframe, pd_script):
    ls_schema = pd_script.get('schema_name')
    ls_table = pd_script.get('table_name')
    ls_bucket = pd_script.get('bucket_name')
    ls_output_path = pd_script.get('output_path')
    ls_data_loc = pd_script.get('data_location')
    ls_key_column = pd_script.get('key_column')
    ls_key_type = pd_script.get('key_col_type')
    ls_col_prefix = pd_script.get('column_prefix', '')
    ls_col_suffix = pd_script.get('column_suffix', '')

    ld_date = datetime.today().strftime('%Y%m%d')
    lv_tbl_tmp = "{0}.{1}_{2}".format(ls_schema, ls_table, ld_date)

    # Drop and Create Temporary table
    lv_ddl = "DROP TABLE IF EXISTS {0} ; \n".format(lv_tbl_tmp)
    lv_ddl = lv_ddl + create_ddl_script(pd_dframe, lv_tbl_tmp, ls_key_column, True)

    # Insert data into the temp table
    lv_ddl = lv_ddl + "    COPY {0}  \n".format(lv_tbl_tmp)
    lv_ddl = lv_ddl + "    FROM '{0}' \n".format(ls_data_loc)
    lv_ddl = lv_ddl + "IAM_ROLE '{iam}' \n"
    lv_ddl = lv_ddl + "  FORMAT AS PARQUET ;\n\n"

    # Drop and ReCreate final table with SELECT INTO - Change date columns to VARCHAR
    lv_tbl_qlfr = ls_schema + "." + ls_table

    lv_ddl = lv_ddl + "DROP TABLE IF EXISTS {0} ; \n".format(lv_tbl_qlfr)
    lv_ddl = lv_ddl + create_select_script(pd_dframe, lv_tbl_tmp, lv_tbl_qlfr, ls_key_column,
                                           ls_key_type, ls_col_prefix, ls_col_suffix)

    # Drop the temp table created.
    lv_ddl = lv_ddl + "DROP TABLE IF EXISTS {0} ; \n".format(lv_tbl_tmp)

    ls_file_name = ls_output_path + ls_table + ".sql"
    if not write_2_s3(pl_logger, ls_bucket, ls_file_name, lv_ddl):
        pl_logger.error("Write DDL Script to S3 failed")
    return


def create_select_script(pd_dframe, ps_src_tbl, ps_tgt_tbl, ps_key_col, ps_key_type,
                         ps_col_prefix="",
                         ps_col_suffix=""):
    lv_ddl = "CREATE TABLE {} \n".format(ps_tgt_tbl)
    lv_ddl = lv_ddl + "DISTSTYLE KEY DISTKEY ({}) \n".format(ps_key_col)
    lv_ddl = lv_ddl + "SORTKEY ({}) \n AS \n".format(ps_key_col)
    lv_ddl = lv_ddl + "SELECT \n"
    for col_name in pd_dframe:
        ls_src_col = str(col_name.name)
        # start changes - do not add prefix/suffix to raw tables and key_col
        if "trans" in ps_tgt_tbl:
            ls_tgt_col = str(col_name.name)
        elif ls_src_col == ps_key_col:
            ls_tgt_col = str(col_name.name)
        else:
            ls_tgt_col = ps_col_prefix + str(col_name.name) + ps_col_suffix
        # end changes - do not add prefix/suffix to raw tables and key_col

        # Assign Pre-defined datatype for the Key column
        if ls_src_col == ps_key_col:
            ls_fld = "CAST({1} AS {0}) AS {2}".format(ps_key_type, ls_src_col, ls_tgt_col)
        elif ls_src_col in ("ID", "Table_ID"):
            ls_fld = "CAST(ROUND({0}) AS BIGINT) AS {1}".format(ls_src_col, ls_tgt_col)
        elif str(col_name.dataType) == "DateType":
            ls_fld = "CAST(TO_CHAR({0},'yyyymmdd') AS VARCHAR(8)) AS {1}".format(ls_src_col,
                                                                                 ls_tgt_col)
        elif str(col_name.dataType) in "IntegerType":
            ls_fld = "{0} AS {1}".format(ls_src_col, ls_tgt_col)
        elif str(col_name.dataType) in ("LongType", "FloatType", "DoubleType"):
            ls_fld = "CAST(ROUND({0}) AS BIGINT) AS {1}".format(ls_src_col, ls_tgt_col)
        elif str(col_name.dataType) in "StringType":
            ls_fld = "CAST({0} AS VARCHAR(100)) AS {1}".format(ls_src_col, ls_tgt_col)
        else:
            ls_fld = "CAST(ROUND({0}) AS INT) AS {1}".format(ls_src_col, ls_tgt_col)
        lv_ddl = lv_ddl + "\t{0}, \n".format(ls_fld.ljust(45, ' '))
        # End of loop

    lv_ddl = lv_ddl[:-3] + "\n   FROM {0} ; \n\n".format(ps_src_tbl)

    lv_ddl = lv_ddl + "COMMIT ;\n\n"

    return lv_ddl


def create_ddl_script(pd_dframe, ps_tbl_name, ls_key_column, pb_drop_flag=True):
    lv_ddl = "CREATE TABLE {0} ( \n".format(ps_tbl_name)
    for col_info in pd_dframe:
        ls_fld = str(col_info.name).ljust(45, ' ')
        if str(col_info.dataType) == "StringType":
            ls_dtype = "VARCHAR(100)"
        elif str(col_info.dataType) == "IntegerType":
            ls_dtype = "INT "
        elif str(col_info.dataType) == "LongType":
            ls_dtype = "BIGINT "
        elif str(col_info.dataType) == "FloatType":
            ls_dtype = "REAL "
        elif str(col_info.dataType) == "DoubleType":
            ls_dtype = "DOUBLE PRECISION"
        elif str(col_info.dataType) == "DateType":
            ls_dtype = "DATE"
        else:
            ls_dtype = str(col_info.dataType)

        lv_ddl = lv_ddl + "\t{0} {1} , \n".format(ls_fld, ls_dtype.ljust(15, ' '))
    lv_ddl = lv_ddl[:-3]
    lv_ddl_dist_key = "DISTSTYLE KEY DISTKEY ({})".format(ls_key_column)
    lv_ddl_sort_key = "SORTKEY ({})".format(ls_key_column)
    lv_ddl = lv_ddl + ")  \n\t{} \n\t{}  ; \n\n\n ".format(lv_ddl_dist_key, lv_ddl_sort_key)
    return lv_ddl


def write_2_s3(pl_logger, ps_bucket, ps_key, ps_content):
    # Write using Boto3
    pl_logger.info(ps_content)
    s3 = boto3.resource('s3')
    obj = s3.Object(ps_bucket, ps_key)
    result = obj.put(Body=ps_content)
    res = result.get('ResponseMetadata')
    if res.get("HTTPStatusCode") != 200:
        return False
    return True


# Write the output data in parquet format on S3
def write_2_file(pl_logger, pdf_gen_tbl, ps_file_name, ps_partition=0, ps_ftype="parquet"):
    pl_logger.info("write2file:{0} - {1} - {2} ".format(pdf_gen_tbl, ps_file_name, ps_partition))
    if ps_ftype == "parquet":
        if ps_partition == 0:
            pdf_gen_tbl.write.mode('overwrite').parquet(ps_file_name)
        else:
            pdf_gen_tbl.repartition(ps_partition).write.mode('overwrite').parquet(ps_file_name)
    elif ps_ftype == "csv":
        pdf_gen_tbl.repartition(ps_partition) \
            .write.mode('overwrite') \
            .csv(path=ps_file_name, sep='|', nullValue=None, emptyValue='', escapeQuotes=None)
    return


# Create DataFrame Structure based on the list Provided.
def fn_create_structure(pl_collist, pb_createID, pb_Reject=False):
    ll_col_list = StructType()
    if pb_createID:
        ll_col_list.add(StructField("ID", dataType=LongType(), nullable=False))
    for col_info in pl_collist:
        if str(col_info[1]) == "Integer":
            st_temp = StructField(col_info[0], dataType=IntegerType(), nullable=True)
        elif str(col_info[1]) == "Long":
            st_temp = StructField(col_info[0], dataType=LongType(), nullable=True)
        elif str(col_info[1]) == "Float":
            st_temp = StructField(col_info[0], dataType=FloatType(), nullable=True)
        elif str(col_info[1]) == "Short":
            st_temp = StructField(col_info[0], dataType=ShortType(), nullable=True)
        elif str(col_info[1]) == "String":
            st_temp = StructField(col_info[0], dataType=StringType(), nullable=True)
        elif str(col_info[1]) == "Date":
            st_temp = StructField(col_info[0], dataType=DateType(), nullable=True)
        else:
            st_temp = StructField(col_info[0], dataType=StringType(), nullable=True)
        ll_col_list.add(st_temp)

    if pb_Reject:
        st_temp = StructField("_corrupt_record", dataType=StringType())
        ll_col_list.add(st_temp)

    return ll_col_list


# Read CSV/PARQUET File and return as Spark DataFrame
def fn_file_2_df(pl_logger, ps_spark, ps_data_file, ps_json_file="", ps_file_format="parquet",
                 ps_rej_loc="", ps_rej_per=0.05):
    ln_rej_per = ps_rej_per
    if ps_file_format == "fixed":
        la_col_list = ["col.Name", "col.Datatype", "col.Length", "col.ColNo", "col.Format"]
        df_tran_hdr = json_2_df(ps_spark, ps_json_file, la_col_list).collect()
        lf_spark_Data = ps_spark.read.text(ps_data_file)
        lf_spark_Data.createOrReplaceTempView("input_data")
        ln_start = 0
        lv_sql = ""
        for rec in df_tran_hdr:
            ls_col = rec['Name']
            ln_len = rec['Length']
            ls_type = rec['Datatype']
            ls_format = rec['Format']
            ls_cur_col = "SUBSTR(value, {0}, {1})".format(ln_start + 1, ln_len)

            if ls_type == 'VARCHAR':
                ls_cast = ", CAST({0} AS {1}({2})) AS {3} ".format(ls_cur_col, ls_type, ln_len,
                                                                   ls_col)
            elif ls_type == 'FLOAT':
                ls_cast = ", CAST({0} AS {1}) AS {2}".format(ls_cur_col, ls_type, ls_col)
            elif ls_type == 'DATE':
                ls_cast = ", TO_DATE({0}, '{1}') AS {2}".format(ls_cur_col, ls_format, ls_col)
            elif ls_type == 'TIMESTAMP':
                ls_cast = ", TO_TIMESTAMP({0}, '{1}') AS {2}".format(ls_cur_col, ls_format, ls_col)
            else:
                ls_cast = ", CAST({0} AS {1}) AS {2}".format(ls_cur_col, ls_type, ls_col)

            lv_sql = lv_sql + ls_cast
            ln_start = ln_start + ln_len
        lv_sql = 'SELECT ' + lv_sql.lstrip(',') + " FROM input_data"
        return ps_spark.sql(lv_sql)

    elif ps_file_format == "csv":
        df_tran_hdr = json_2_df(ps_spark, ps_json_file, ["col.Name", "col.Datatype", "col.ColNo"])
        lst_cols = df_tran_hdr.sort("ColNo").select("Name", "DataType").collect()
        meta_columns = fn_create_structure(lst_cols, pb_createID=False, pb_Reject=True)
        lf_spark_data = ps_spark.read.option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("dateFormat", "yyyyMMdd") \
            .csv(ps_data_file, sep='|', schema=meta_columns)
        lf_spark_data.cache()
        showdata(pl_logger, ps_spark, lf_spark_data)
        ln_input_cnt = lf_spark_data.count()
        pl_logger.info("Total Records Processed : {}".format(ln_input_cnt))
        lf_reject = lf_spark_data.filter("_corrupt_record IS NOT NULL") \
            .select("_corrupt_record") \
            .withColumnRenamed("_corrupt_record", "BadRecord")
        ln_rej_cnt = lf_reject.count()
        if ln_rej_cnt > 0:
            pl_logger.warning("{0} Bad records found on Incoming dataset!!!".format(ln_rej_cnt))
            lf_reject.createOrReplaceTempView("rejData")
            pl_logger.info(showdata(pl_logger, ps_spark, "rejData"))
            if len(ps_rej_loc) > 0:
                write_2_file(pl_logger, lf_reject, ps_rej_loc, 1, "csv")

        if ln_rej_cnt / ln_input_cnt > ln_rej_per:
            pl_logger.error("Rejects exceeded {0}%. Halting process!!!".format(ln_rej_per))
            raise "Rejects exceeded {0}%. Halting process!!!".format(ln_rej_per)
        lf_spark_data = lf_spark_data.filter("_corrupt_record IS NULL").drop("_corrupt_record")
        ln_process_cnt = lf_spark_data.count()
        pl_logger.info("Total Valid Records : {}".format(ln_process_cnt))
        return lf_spark_data
    elif ps_file_format == "parquet":
        lf_spark_data = ps_spark.read.parquet(ps_data_file)
        return lf_spark_data
    return None


def showdata(pl_logger, ps_spark, ps_table_name, pn_rows=10):
    pl_logger.info("-" * 150)
    if type(ps_table_name) is str:
        pl_logger.info("Describing table {0} [Sampling {1} Rows] ".format(ps_table_name, pn_rows))
        pl_logger.info("-" * 150)
        pl_logger.info(ps_spark.sql("select * from " + ps_table_name).show(pn_rows, False))
    else:
        pl_logger.info("Describing table {0} [Sampling {1} Rows] ".format(ps_table_name, pn_rows))
        pl_logger.info(ps_table_name.show(pn_rows, False))
    pl_logger.info("-" * 150)


# Read Config file [JSON Format]
def read_json_file(pl_logger, ps_bucket_name, ps_key, pd_date):
    s3 = boto3.resource('s3')
    ls_json_content = s3.Object(ps_bucket_name, ps_key)
    ls_content = ls_json_content.get()['Body'].read().decode('utf-8')
    ld_date = datetime.strptime(pd_date, '%Y%m%d').date()
    ln_year = ld_date.strftime('%Y')
    ln_mnth = ld_date.strftime('%m')
    ln_day = ld_date.strftime('%d')
    ls_content = ls_content.replace('{yyyy}', ln_year) \
        .replace('{mm}', ln_mnth) \
        .replace('{dd}', ln_day)
    pl_logger.info(ls_content)
    return json.loads(ls_content)


# Read Config file [JSON Format]
def read_json_file_local(pl_logger, ps_file_name, pd_date):
    ls_content = (open(ps_file_name)).read()
    ld_date = datetime.strptime(pd_date, '%Y%m%d').date()
    ln_year = ld_date.strftime('%Y')
    ln_mnth = ld_date.strftime('%m')
    ln_day = ld_date.strftime('%d')
    ls_content = ls_content.replace('{yyyy}', ln_year) \
        .replace('{mm}', ln_mnth) \
        .replace('{dd}', ln_day)
    pl_logger.info(ls_content)
    return json.loads(ls_content)


# Create Dataframe based on the metadata passed as JSON format
def json_2_df(ps_spark, p_file_name, *kw_col_list, ps_heading="Records"):
    tmp_df = ps_spark.read.option("multiline", "True").json(p_file_name)
    return tmp_df.select(f.explode(ps_heading)).selectExpr(*kw_col_list)


# Write the Discarded data in parquet format on S3
def fn_discard_data(pl_logger, ps_spark, pdf_input, ps_json_file, ls_dsc_loc, ps_run_dt):
    df_tran_hdr = json_2_df(ps_spark, ps_json_file, ["col.Formula"], ps_heading="Filter")
    ls_sql = ""
    ld_run_dt = datetime.strptime(ps_run_dt, '%Y%m%d').strftime('%Y-%m-%d')
    for rec in df_tran_hdr.collect():
        ls_sql = ls_sql + " " + rec['Formula']
    ls_sql = ls_sql.strip().lstrip('AND').replace('{RunDate}', ld_run_dt)
    ls_sql = ls_sql.lstrip('OR')
    ldf_discard = pdf_input.filter('NOT (' + ls_sql + ')')

    write_2_file(pl_logger, ldf_discard, ls_dsc_loc, 1, 'csv')

    lf_spark_data = pdf_input.filter(ls_sql)
    return lf_spark_data


def fn_gen_sql(ps_ColList):
    lv_gensql = ""
    for ls_rec in ps_ColList:
        lv_gensql = lv_gensql + ' ' + str(ls_rec['ColList'])
    return lv_gensql


def gen_outer_join(pl_logger, ps_spark, ps_base_tbl, ps_base_col, ps_curr_tbl, ps_curr_col):
    pl_logger.info("Create Outer Join ")
    ls_cmp_id = "COALESCE( " + ps_base_col + ", " + ps_curr_col + ") as Process_ID, "
    ls_From = " FROM " + ps_base_tbl
    ls_Join = " FULL OUTER JOIN " + ps_curr_tbl
    ls_JoinKey = " ON " + ps_base_tbl + "." + ps_base_col + " = " + ps_curr_tbl + "." + ps_curr_col
    ls_Order = " ORDER BY COALESCE( " + ps_base_col + ", " + ps_curr_col + ") "
    ls_final_sql = "SELECT " + ls_cmp_id + " * " + ls_From + ls_Join + ls_JoinKey + ls_Order
    pl_logger.info("Outer Join SQL: {0}".format(ls_final_sql))
    df_cat_tbl = ps_spark.sql(ls_final_sql)
    df_cat_tbl = df_cat_tbl.withColumnRenamed(ps_base_col, "ToDrop") \
        .withColumnRenamed("Process_ID", ps_base_col).drop("ToDrop").drop(ps_curr_col)
    return df_cat_tbl
