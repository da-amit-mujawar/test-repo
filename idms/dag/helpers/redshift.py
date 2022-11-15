from airflow.hooks.jdbc_hook import JdbcHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import boto3, json
from helpers.sqlserver import get_sqlserver_hook

sqlhook = get_sqlserver_hook()
def get_sqlserver_listid(databaseid):
    sql = f"""SELECT b.ID as listid FROM DW_Admin.dbo.tblBuildLoL a (nolock) 
    INNER JOIN DW_Admin.dbo.tblMasterLoL b (nolock) 
    on a.MasterLoLID = b.ID WHERE b.DatabaseID = {databaseid} """
    sql_result = sqlhook.run(sql)
    return sql_result



def get_redshift_hook(**kwargs):
    db_conn = PostgresHook(postgres_conn_id=Variable.get("var-redshift-postgres-conn"))
    return db_conn


def getconnection(**kwargs):
    if "connection" in kwargs:
        connection_var = kwargs["connection"]
    else:
        connection_var = "var-redshift-postgres-conn"

    db_conn = PostgresHook(postgres_conn_id=Variable.get(connection_var))
    return db_conn


def executenonquery(sql, **kwargs):
    db_conn = getconnection(**kwargs)

    if "iam" in kwargs:
        iam_role_var = kwargs["iam"]
    else:
        iam_role_var = "var-password-redshift-iam-role"

    if "kms_key" in kwargs:
        kms_key_var = kwargs["kms_key"]
    else:
        kms_key_var = "var-password-kms-key"

    sql = sql.replace("{iam}", Variable.get(iam_role_var)).replace(
        "{kmskey}", Variable.get(kms_key_var)
    )
    db_conn.run(sql)


def executequery(sql, **kwargs):
    db_conn = getconnection(**kwargs)
    if "iam" in kwargs:
        iam_role_var = kwargs["iam"]
    else:
        iam_role_var = "var-password-redshift-iam-role"

    if "kms_key" in kwargs:
        kms_key_var = kwargs["kms_key"]
    else:
        kms_key_var = "var-password-kms-key"

    sql = sql.replace("{iam}", Variable.get(iam_role_var)).replace(
        "{kmskey}", Variable.get(kms_key_var)
    )
    records = db_conn.get_records(sql)
    return records


def countquery(tablename, **kwargs):
    db_conn = getconnection(**kwargs)
    sql = f"select count(*) from {tablename}"
    record_count = db_conn.get_records(sql)
    return record_count[0][0]


def generate_manifest(folder_list, output_bucket, output_path):
    input_list = []
    s3 = boto3.resource("s3")
    for folder in folder_list:
        bucket = s3.Bucket(folder["bucket"])
        prefix = folder["prefix"]
        for file in bucket.objects.filter(Prefix=prefix):
            input_list.append(f"s3://{bucket.name}/{file.key}")

    generate_manifest_json(output_bucket, output_path, input_list)


def generate_manifest_json(output_bucket, output_path, input_list):
    final_dict = {"entries": []}
    finallist = []
    for l in input_list:
        tempdict = {}
        tempdict["url"] = l
        tempdict["mandatory"] = True
        finallist.append(tempdict)
    final_dict["entries"] = finallist

    s3 = boto3.resource("s3")
    s3object = s3.Object(output_bucket, output_path)

    s3object.put(Body=(bytes(json.dumps(final_dict, indent=4).encode("UTF-8"))))


def get_create_table_ddl(tname, sch_df,databaseid = 0):
    sql_result = get_sqlserver_listid(databaseid)
    ddl = f"""DROP TABLE IF EXISTS {tname};
        CREATE TABLE {tname} (
        """

    for index, row in sch_df.iterrows():
        c = row["column_name"].strip().lower()
        ctype = row["column_type"].strip().lower()
        cencode = row["encoding"].strip().lower()
        csortkey = row["sort_key"].strip().lower()
        cdistkey = row["dist_key"].strip().lower()
        cdefault = row["default_column"].strip().lower()

        if ctype == "varchar":
            clen = str(int(row["column_length"])).strip()
            ddl += f"""{c} {ctype}({clen})"""
        else:
            ddl += f"""{c} {ctype}"""

        if cencode != "":
            ddl += f""" ENCODE {cencode}"""
            
        if cdefault == "y":
            ddl += f" default {sql_result}"

        if cdistkey == "y":
            ddl += " DISTKEY"

        if csortkey == "y":
            ddl += " SORTKEY"

        ddl += ","

    ddl = ddl[:-1] + ");"

    return ddl


def copy_table_using_manifest(
    all_files, input_suffix, s3_path, s3_path_temp, manifest_file
):
    input_list = []
    for file in all_files:
        if file.key.endswith(input_suffix) and "sample" not in file.key.lower():
            folder = {"bucket": s3_path, "prefix": file.key}
            input_list.append(folder)

    generate_manifest(input_list, s3_path_temp, manifest_file)
    
def renametable(SourceTableName, TargetTableName):
    query = f"""SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '{SourceTableName}')"""
    query_result = executequery(query)
    if query_result[0][0]:
        sql1 = f"""DROP TABLE IF EXISTS {TargetTableName};
            ALTER TABLE {SourceTableName} RENAME TO {TargetTableName};"""
        executenonquery(sql1)

def create_empty_table(rs_tablename,temp_tablename,redshift_connection_var,redshift_iam_role_var):
    Create_table = f"CREATE TABLE {rs_tablename} AS SELECT * FROM {temp_tablename} WHERE 1=2;"
    executenonquery(Create_table,connection = redshift_connection_var, iam = redshift_iam_role_var)

def insert_into_table(rs_tablename,temp_tablename,redshift_connection_var,redshift_iam_role_var):
    Insert_Command = f"INSERT INTO {rs_tablename} SELECT * FROM {temp_tablename}"
    executenonquery(Insert_Command,connection = redshift_connection_var, iam = redshift_iam_role_var)

def append_table(rs_tablename,temp_tablename,deletecolumn,redshift_connection_var,redshift_iam_role_var):
    query = f"""SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = lower('{rs_tablename}'))"""
    query_result = executequery(query,connection = redshift_connection_var, iam = redshift_iam_role_var)
    if query_result[0][0]:
        pass
    else:
        create_empty_table(rs_tablename,temp_tablename,redshift_connection_var,redshift_iam_role_var)
    
    Delete_Command = f"DELETE FROM {rs_tablename} WHERE {deletecolumn} IN (SELECT DISTINCT {deletecolumn} FROM {temp_tablename});"
    executenonquery(Delete_Command,connection = redshift_connection_var, iam = redshift_iam_role_var)

    insert_into_table(rs_tablename,temp_tablename,redshift_connection_var,redshift_iam_role_var)


