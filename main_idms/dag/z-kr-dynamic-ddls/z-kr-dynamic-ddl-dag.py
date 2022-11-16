from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import os, subprocess
import json
import boto3
import gzip
import pandas as pd
from datetime import datetime
import calendar
from operators import RedshiftOperator, GenericRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 2),
    'depends_on_past': False,
    'currentpath': os.path.abspath(os.path.dirname(__file__)),
    'retries': 0,
    'email_on_retry': False
}

dag = DAG('z-kr-dynamic-ddl-dag',
          default_args=default_args,
          description='Dynamic generation of DDL',
          schedule_interval='@once',
          max_active_runs=1
          )

config_file_path=default_args['currentpath'] + '/config.json'
with open(config_file_path) as f:
    config = json.load(f)

def fetch_header(bucket,file):
    filename = "s3://"+bucket.name+"/"+file.key
    cmd = "aws s3 cp "+filename+" - | zcat | head -n 1"
    ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    header_string = ps.communicate()[0].decode("utf-8").split("\n",1)[0].strip()
    print('header for file: ', file.key,' is ',header_string )
    return header_string

def create_ddls(file_types_list, bucket, mode, refer_df):
    keywords_list = ['AES128','AES256','ALL','ALLOWOVERWRITE','ANALYSE','ANALYZE','AND','ANY','ARRAY','AS','ASC','AUTHORIZATION','AZ64','BACKUP','BETWEEN','BINARY','BLANKSASNULL','BOTH','BYTEDICT','BZIP2','CASE','CAST','CHECK','COLLATE','COLUMN','CONSTRAINT','CREATE','CREDENTIALS','CROSS','CURRENT_DATE','CURRENT_TIME','CURRENT_TIMESTAMP','CURRENT_USER','CURRENT_USER_ID','DEFAULT','DEFERRABLE','DEFLATE','DEFRAG','DELTA','DELTA32K','DESC','DISABLE','DISTINCT','DO','ELSE','EMPTYASNULL','ENABLE','ENCODE','ENCRYPT','ENCRYPTION','END','EXCEPT','EXPLICIT','FALSE','FOR','FOREIGN','FREEZE','FROM','FULL','GLOBALDICT256','GLOBALDICT64K','GRANT','GROUP','GZIP','HAVING','IDENTITY','IGNORE','ILIKE','IN','INITIALLY','INNER','INTERSECT','INTO','IS','ISNULL','JOIN','LANGUAGE','LEADING','LEFT','LIKE','LIMIT','LOCALTIME','LOCALTIMESTAMP','LUN','LUNS','LZO','LZOP','MINUS','MOSTLY16','MOSTLY32','MOSTLY8','NATURAL','NEW','NOT','NOTNULL','NULL','NULLS','OFF','OFFLINE','OFFSET','OID','OLD','ON','ONLY','OPEN','OR','ORDER','OUTER','OVERLAPS','PARALLEL','PARTITION','PERCENT','PERMISSIONS','PLACING','PRIMARY','RAW','READRATIO','RECOVER','REFERENCES','RESPECT','REJECTLOG','RESORT','RESTORE','RIGHT','SELECT','SESSION_USER','SIMILAR','SNAPSHOT','SOME','SYSDATE','SYSTEM','TABLE','TAG','TDES','TEXT255','TEXT32K','THEN','TIMESTAMP','TO','TOP','TRAILING','TRUE','TRUNCATECOLUMNS','UNION','UNIQUE','USER','USING','VERBOSE','WALLET','WHEN','WHERE','WITH','WITHOUT']
    cdate = datetime.today().strftime('%Y%m%d')
    #cdate = 20210310
    print(str(cdate))
    print(config.get('prefix'))
    for fname in file_types_list:
        print("in for ",fname)
        ddl = ''
        print(config.get('prefix')+str(cdate))
        for file in bucket.objects.filter(Prefix=config.get('prefix')+str(cdate)):
            print('file name: ',file)
            if file.key.endswith(config.get('suffix')) and fname in file.key:
                ddl = f"""DROP TABLE IF EXISTS core_bf.{fname}_{mode};"""
                ddl = ddl + f"""
CREATE TABLE core_bf.{fname}_{mode}
( """
                header_str = fetch_header(bucket,file)
                header_str = header_str.replace(".","_")
                header_list = header_str.split("|")
                filter_df = refer_df[refer_df['file_type'] == fname]
                #filter_df.display()
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

                        if h.upper() in keywords_list:
                            print('keyword found: ',h)
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
diststyle ALL
;"""
        outfile_name = default_args['currentpath'] + config.get('output_path')+"bf_"+mode+"_"+fname+"_ddl.sql"
        with open(outfile_name, "w") as text_file:
            text_file.write(ddl)

        infile_name = default_args['currentpath'] + config.get('output_path')+"bf_"+mode+"_"+fname+"_ddl.sql"
        with open(infile_name, "r") as text_file:
            print("Written on local in file "+infile_name+" "+text_file.read())

def generate_ddl_function():
    #Read CSV file for schema
    # s3 = boto3.client('s3')
    # obj = s3.get_object(Bucket= config.get('refer_bucket'), Key= config.get("refer_file_name"))
    # refer_df = pd.read_csv(obj['Body'])
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
        create_ddls(file_types_list, bucket, mode, refer_df)

generate_ddl = PythonOperator(
    task_id='generate_ddl_task',
    python_callable=generate_ddl_function,
    provide_context=True,
    dag=dag,
)

generate_ddl