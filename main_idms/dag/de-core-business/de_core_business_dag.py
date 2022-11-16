from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from operators import RedshiftOperator, GenericRedshiftOperator
import os, json, boto3, calendar, subprocess
import pandas as pd
from datetime import datetime
from helpers.send_email import send_email
from concurrent import futures

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 31),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False,
    "email": "DL-DataEngneering@data-axle.com",
}

dag = DAG('de-core-business',
          default_args=default_args,
          description='This DAG populates business data in Redshift.',
          schedule_interval='0 2 * * *',
          max_active_runs=1
          )

config_file_path=default_args['currentpath'] + '/config.json'
with open(config_file_path) as f:
    config = json.load(f)

def fetchToday():
    today_date = datetime.today()
    today_formated = today_date.strftime('%Y%m%d')
    day = calendar.day_name[datetime.today().weekday()]
    return today_formated, day

def fetch_header(bucket,file):
    filename = "s3://"+bucket.name+"/"+file.key
    cmd = "aws s3 cp "+filename+" - | zcat | head -n 1"
    ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    header_string = ps.communicate()[0].decode("utf-8").split("\n",1)[0].strip()
    print('header for file: ', file.key,' is ',header_string )
    return header_string

def create_ddls(file_types_list, bucket, mode, refer_df, **kwargs):
    keywords_list = ['AES128','AES256','ALL','ALLOWOVERWRITE','ANALYSE','ANALYZE','AND','ANY','ARRAY','AS','ASC','AUTHORIZATION','AZ64','BACKUP','BETWEEN','BINARY','BLANKSASNULL','BOTH','BYTEDICT','BZIP2','CASE','CAST','CHECK','COLLATE','COLUMN','CONSTRAINT','CREATE','CREDENTIALS','CROSS','CURRENT_DATE','CURRENT_TIME','CURRENT_TIMESTAMP','CURRENT_USER','CURRENT_USER_ID','DEFAULT','DEFERRABLE','DEFLATE','DEFRAG','DELTA','DELTA32K','DESC','DISABLE','DISTINCT','DO','ELSE','EMPTYASNULL','ENABLE','ENCODE','ENCRYPT','ENCRYPTION','END','EXCEPT','EXPLICIT','FALSE','FOR','FOREIGN','FREEZE','FROM','FULL','GLOBALDICT256','GLOBALDICT64K','GRANT','GROUP','GZIP','HAVING','IDENTITY','IGNORE','ILIKE','IN','INITIALLY','INNER','INTERSECT','INTO','IS','ISNULL','JOIN','LANGUAGE','LEADING','LEFT','LIKE','LIMIT','LOCALTIME','LOCALTIMESTAMP','LUN','LUNS','LZO','LZOP','MINUS','MOSTLY16','MOSTLY32','MOSTLY8','NATURAL','NEW','NOT','NOTNULL','NULL','NULLS','OFF','OFFLINE','OFFSET','OID','OLD','ON','ONLY','OPEN','OR','ORDER','OUTER','OVERLAPS','PARALLEL','PARTITION','PERCENT','PERMISSIONS','PLACING','PRIMARY','RAW','READRATIO','RECOVER','REFERENCES','RESPECT','REJECTLOG','RESORT','RESTORE','RIGHT','SELECT','SESSION_USER','SIMILAR','SNAPSHOT','SOME','SYSDATE','SYSTEM','TABLE','TAG','TDES','TEXT255','TEXT32K','THEN','TIMESTAMP','TO','TOP','TRAILING','TRUE','TRUNCATECOLUMNS','UNION','UNIQUE','USER','USING','VERBOSE','WALLET','WHEN','WHERE','WITH','WITHOUT']
    input_prefix_full = config.get('input_prefix_full')
    input_prefix_changes = config.get('input_prefix_changes')
    input_prefix = input_prefix_changes

    if kwargs['dag_run'].conf.get('run_date') == None or kwargs['dag_run'].conf.get('run_day') == None:
        cdate, day = fetchToday()
    else:
        cdate = kwargs['dag_run'].conf.get('run_date')
        day = kwargs['dag_run'].conf.get('run_day')
    # cdate = 20210314
    # day = "Sunday"

    if day == config.get("full_load_day"):
        input_prefix = input_prefix_full
    else:
        input_prefix = input_prefix_changes

    print(str(cdate))
    for fname in file_types_list:
        ddl = ''
        for file in bucket.objects.filter(Prefix=input_prefix+str(cdate)):
            print('file name: ',file)
            if file.key.endswith(config.get('file_extension')) and '_'+fname in file.key:
                ddl = f"""DROP TABLE IF EXISTS core_bf.{fname}_{mode};"""

                ddl = ddl + f"""
CREATE TABLE core_bf.{fname}_{mode}
( """
                header_str = fetch_header(bucket,file)
                header_str = header_str.replace(".","_")
                header_list = header_str.split("|")
                filter_df = refer_df[refer_df['file_type'] == fname]
                count = 0
                for h in header_list:
                    if h in filter_df.values:
                        count += 1
                        f_df = filter_df[filter_df['column_name'] == h]
                        ctype = (f_df['column_type']).to_string(index=False).strip().lower()
                        clength = str((f_df['column_length']).to_string(index=False).strip()).split('.')[0]
                        encode = (f_df['encoding']).to_string(index=False).strip()
                        pkey = (f_df['primary_key']).to_string(index=False).strip()
                        notnull = (f_df['not_null']).to_string(index=False).strip()
                        sortkey = (f_df['sort_key']).to_string(index=False).strip()

                        if fname == "places" and mode == "new" and h == "change_type":
                            pass
                        else:
                            h = "\""+h+"\""

                            if ctype == "varchar":
                                ddl += f"""
{h} {ctype}({clength})"""
                            else:
                                ddl += f"""
{h} {ctype}"""
                            if notnull == "y":
                                ddl += " NOT NULL"
                            if sortkey == "y":
                                ddl += " SORTKEY"
                            if pkey == "y":
                                ddl += " PRIMARY KEY"
                            if encode != "NA":
                                ddl += f""" ENCODE {encode}"""
                            ddl += ","
                print("file type ",fname," has ",count," columns")
                print("Total columns in file header for ",fname," is ",len(header_list))
                break
        ddl = ddl[:-1]
        ddl = ddl + f"""
)
diststyle key
distkey(infogroup_id)
;"""
        outfile_name = default_args['currentpath'] + config.get('output_path2')+"bf_"+mode+"_"+fname+"_ddl.sql"
        with open(outfile_name, "w") as text_file:
            text_file.write(ddl)

        infile_name = default_args['currentpath'] + config.get('output_path2')+"bf_"+mode+"_"+fname+"_ddl.sql"
        with open(infile_name, "r") as text_file:
            print("Written on local in file "+infile_name+" "+text_file.read())

def generate_ddl_function(**kwargs):
    #Read CSV file for schema
    refer_file_path=default_args['currentpath'] + '/refer_full.csv'
    refer_df = pd.read_csv(refer_file_path)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(config.get("input_bucket"))

    file_types = config['file_types']
    file_types_dict = json.loads(file_types)
    file_types_list = file_types_dict.keys()

    modes = config.get('modes')
    modes_list = modes.split(',')

    for mode in modes_list:
        create_ddls(file_types_list, bucket, mode, refer_df, **kwargs)

def generateList(bucket, prefix, suffix, date, output_bucket, output_path):
    input_list = []
    for file in bucket.objects.filter(Prefix=prefix+str(date)):
        if file.key.endswith(suffix) and "sample" not in file.key.lower():
            print(file.key)
            input_list.append("s3://"+bucket.name+"/"+file.key)
    print('list after getting files :',input_list)
    generateJSON(output_bucket, output_path, input_list)

def generateJSON(output_bucket, output_path, input_list):
    final_dict = {"entries": []}
    finallist = []
    for l in input_list:
        tempdict = {}
        tempdict['url']=l
        tempdict['mandatory']=True
        finallist.append(tempdict)
    final_dict["entries"]=finallist

    s3 = boto3.resource('s3')
    s3object = s3.Object(output_bucket, output_path)

    s3object.put(
        Body=(bytes(json.dumps(final_dict, indent = 4).encode('UTF-8')))
    )

def generateManifestFile(**kwargs):
    s3 = boto3.resource('s3')
    input_bucket = s3.Bucket(config.get('input_bucket'))
    input_prefix_full = config.get('input_prefix_full')
    input_prefix_changes = config.get('input_prefix_changes')
    file_types = config['file_types']
    file_types_dict = json.loads(file_types)
    file_types_list = file_types_dict.keys()
    mode = "create_full_table"

    if kwargs['dag_run'].conf.get('run_date') == None or kwargs['dag_run'].conf.get('run_day') == None:
        date, day = fetchToday()
    else:
        date = kwargs['dag_run'].conf.get('run_date')
        day = kwargs['dag_run'].conf.get('run_day')
    # date,day = fetchToday()
    # date = 20210314
    # day = "Sunday"
    if day == config.get("full_load_day"):
        input_prefix = input_prefix_full
        mode = "create_full_table"
    else:
        input_prefix = input_prefix_changes
        mode = "create_changes_table"
    for file_type in file_types_list:
        input_suffix = file_type+config.get('file_extension')
        output_bucket = Variable.get('var-manifest-bucket')
        output_path = Variable.get('var-manifest-path')+file_type+'_manifest.json'
        generateList(input_bucket, input_prefix, input_suffix, date, output_bucket, output_path)
    return mode


def generateSetters(file_type, file_types_dict):
    ddl_file=default_args['currentpath'] + '/DDL/bf_new_'+file_type+'_ddl.sql'
    fd2 = open(ddl_file, 'r')
    lines = fd2.readlines()
    temp = ""
    lst = []
    for line in lines:
        if temp == "y" and line.strip() != ")":
           l = line.split()
           lst.append(l[0])
        if line.strip() == "(":
            temp = "y"
        if line.strip() == ")":
            temp = ""
    print(lst)

    setters = ""
    for l in lst:
        if l != file_types_dict.get(file_type):
            setters = setters + l+"="+"c."+l+","

    setters = setters[:-1]
    fd2.close()
    print(setters)
    return setters, lst


def upsert_function(**kwargs):
    ignore_list = []
    print(kwargs['redshift_conn_id'])
    file_types = config['file_types']
    file_types_dict = json.loads(file_types)
    print("dictionary is:",file_types_dict)
    file_types_list = file_types_dict.keys()
    print("list is:",file_types_list)

    # insert file types need to be removed as it is taken care of in the insert_function
    insert_file_types = config['insert_file_types']
    insert_file_types_dict = json.loads(insert_file_types)
    insert_file_types_list = insert_file_types_dict.keys()

    final_file_types_list = list(set(file_types_list) - set(ignore_list) - set(insert_file_types_list))
    print("Final list is:",final_file_types_list)

    #added function to perform parallel deletion of existing data and then insert the data instead of upsert
    def parallel_delete_insert(file_type):
        file_types = config['file_types']
        file_types_dict = json.loads(file_types)
        redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
        sql_file=default_args['currentpath'] + '/SQL/bf_changes_upsert.sql'
        fd = open(sql_file, 'r')
        sql = fd.read()
        fd.close()
        setter_fields, column_list = generateSetters(file_type, file_types_dict)
        columns = ','.join(column_list)
        sql2 = ""
        sql2 = sql.replace('{table}', file_type)
        sql2 = sql2.replace('{set_columns}', setter_fields)
        sql2 = sql2.replace('{column_list}', columns)
        sql2 = sql2.replace('{primary_key_var}', file_types_dict.get(file_type))
        print("executing sql querry:",sql2)
        redshift_hook.run(sql2)

    with futures.ThreadPoolExecutor() as executor:
        result = executor.map(parallel_delete_insert, final_file_types_list)

def insert_function(**kwargs):
    ignore_list = []
    print(kwargs['redshift_conn_id'])
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_changes_insert.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    file_types = config['insert_file_types']
    file_types_dict = json.loads(file_types)
    print("dictionary is:",file_types_dict)
    file_types_list = file_types_dict.keys()
    print("list is:",file_types_list)
    final_file_types_list = list(set(file_types_list) - set(ignore_list))
    print("Final list is:",final_file_types_list)
    for file_type in final_file_types_list:
        #Logic to generate setter columns string
        setter_fields, column_list = generateSetters(file_type, file_types_dict)
        columns = ','.join(column_list)
        sql2 = ""
        sql2 = sql.replace('{table}', file_type)
        sql2 = sql2.replace('{column_list}', columns)
        sql2 = sql2.replace('{primary_key_var}', file_types_dict.get(file_type))
        # self.log.info(f'Executing query: {sql2}')
        print("executing sql querry:",sql2)
        redshift_hook.run(sql2)

def full_postcopy_function(**kwargs):
    print(kwargs['redshift_conn_id'])
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    connection = redshift_hook.get_conn()
    sql_file=default_args['currentpath'] + '/SQL/bf_full_postcopy.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    file_types = config['file_types']
    file_types_dict = json.loads(file_types)
    file_types_list = file_types_dict.keys()
    for file_type in file_types_list:
        select_query = "select 1 from pg_tables where schemaname = 'core_bf' and tablename = '"+file_type+"';"
        cursor = connection.cursor()
        cursor.execute(select_query)
        qresult = cursor.fetchall()
        if qresult:
            sql2 = ""
            sql2 = sql.replace('{table}', file_type)
            redshift_hook.run(sql2)
        else:
            qry = "ALTER TABLE core_bf."+file_type+"_new RENAME TO "+file_type+";"
            redshift_hook.run(qry)

def derive_counts(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_counts.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    file_types = config['file_types']
    file_types_dict = json.loads(file_types)
    file_types_list = file_types_dict.keys()
    for file_type in file_types_list:
        sql2 = ""
        sql2 = sql.replace('{counts_table}', config.get('counts_table'))
        sql2 = sql2.replace('{file_type}', file_type)
        redshift_hook.run(sql2)

def prepare_counts_table(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id=kwargs['redshift_conn_id'])
    sql_file=default_args['currentpath'] + '/SQL/bf_counts_table_ddl.sql'
    fd = open(sql_file, 'r')
    sql = fd.read()
    fd.close()
    sql = sql.replace('{counts_table}', config.get('counts_table'))
    redshift_hook.run(sql)

create_ddl_op = PythonOperator(
    task_id='create_ddl_task',
    python_callable=generate_ddl_function,
    provide_context=True,
    dag=dag,
)

generate_op = BranchPythonOperator(
    task_id='generate_load',
    python_callable=generateManifestFile,
    provide_context=True,
    dag=dag,
)

create_full_table = RedshiftOperator(
    task_id='create_full_table',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/DDL/bf_new_*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

create_changes_table = RedshiftOperator(
    task_id='create_changes_table',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/DDL/bf_changes_*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

full_copy = GenericRedshiftOperator(
    task_id='full_copy',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/SQL/bf_full_copy.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

changes_copy = GenericRedshiftOperator(
    task_id='changes_copy',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/SQL/bf_changes_copy.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

upsert_task = PythonOperator(
    task_id='upsert_task',
    python_callable=upsert_function,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

insert_task = PythonOperator(
    task_id='insert_task',
    python_callable=insert_function,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

full_postcopy = PythonOperator(
    task_id='full_postcopy',
    python_callable=full_postcopy_function,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

full_counts_prepare = PythonOperator(
    task_id='prepare_counts_table_full',
    python_callable=prepare_counts_table,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

full_counts = PythonOperator(
    task_id='counts_after_full_load',
    python_callable=derive_counts,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

changes_counts_prepare = PythonOperator(
    task_id='prepare_counts_table_changes',
    python_callable=prepare_counts_table,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

changes_counts = PythonOperator(
    task_id='counts_after_changes_load',
    python_callable=derive_counts,
    op_kwargs={'redshift_conn_id': Variable.get('var-redshift-da-rs-01-connection')},
    provide_context=True,
    dag=dag,
)

create_feature_store_full = RedshiftOperator(
    task_id='update_feature_store_full',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

create_feature_ds_store_full = RedshiftOperator(
    task_id='create_feature_ds_store_full',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/DataScience/0*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)

create_feature_store_changes = RedshiftOperator(
    task_id='update_feature_store_changes',
    dag=dag,
    sql_file_path=default_args['currentpath'] + '/*.sql',
    config_file_path=default_args['currentpath'] + '/config.json',
    redshift_conn_id=Variable.get('var-redshift-da-rs-01-connection')
)
send_notification_full = PythonOperator(
        task_id='email_notification_full',
        python_callable=send_email,
        op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
                  'dag': dag
               },
        dag=dag)

send_notification_changes = PythonOperator(
        task_id='email_notification_changes',
        python_callable=send_email,
        op_kwargs={'config_file': default_args['currentpath'] + '/config.json',
                  'dag': dag
               },
        dag=dag)



parallel_feature_task = [create_feature_store_full,create_feature_ds_store_full]

create_ddl_op.set_downstream(generate_op)
generate_op >> [create_full_table,create_changes_table]

create_full_table.set_downstream(full_copy)
full_copy.set_downstream(full_postcopy)
full_postcopy.set_downstream(parallel_feature_task)
parallel_feature_task>>full_counts_prepare
full_counts_prepare.set_downstream(full_counts)
full_counts.set_downstream(send_notification_full)

create_changes_table.set_downstream(changes_copy)
changes_copy.set_downstream(upsert_task)
upsert_task.set_downstream(insert_task)
insert_task.set_downstream(create_feature_store_changes)
create_feature_store_changes.set_downstream(changes_counts_prepare)
changes_counts_prepare.set_downstream(changes_counts)
changes_counts.set_downstream(send_notification_changes)

"""
create_ddl_op.set_downstream(generate_op)
generate_op >> [create_full_table,create_changes_table]
create_full_table.set_downstream(full_copy)
create_changes_table.set_downstream(changes_copy)
full_copy.set_downstream(full_postcopy)
full_postcopy.set_downstream(parallel_feature_task)
parallel_feature_task>>full_counts_prepare
full_counts_prepare.set_downstream(full_counts)
full_counts.set_downstream(send_notification)
changes_copy.set_downstream(upsert_task)
upsert_task.set_downstream(changes_postcopy)
changes_postcopy.set_downstream(update_feature_store)
update_feature_store.set_downstream(changes_counts_prepare)
changes_counts_prepare.set_downstream(changes_counts)
changes_counts.set_downstream(send_notification)
"""
