from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.hooks.jdbc_hook import JdbcHook
import json
import glob
from multiprocessing import Pool
import multiprocessing
from billiard.context import Process
from concurrent import futures

class GenericRedshiftOperator(BaseOperator):
    """
    Run sql queries in the specified folder to create and copy table from s3 file to redshift,
    write report to s3, rename table
    Parameters: redshift_conn_id: Redshift connection ID
                aws_credentials_id: AWS credentials ID
                sql_file_path: folder path for the sql files
                config_file_path: folder path for the config file
                table: Target staging table in Redshift to copy data into
                s3_bucket: S3 bucket where input files are located
                s3_key: s3 bucket path for the input files
                region: AWS Region where the input data is located
    """
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 aws_credentials_id='',
                 sql_file_path='',
                 config_file_path='',
                 databaseid=0,
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 *args, **kwargs):
        super(GenericRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here:
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_file_path = sql_file_path
        self.config_file_path = config_file_path
        self.databaseid = databaseid
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        self.log.info('RedshiftOperator is checking credentials')
        self.log.info(self.config_file_path)
        self.log.info(self.sql_file_path)

        #redshift_hook = JdbcHook(jdbc_conn_id=self.redshift_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        current_date = datetime.now()
        self.log.info('loading config files')
        config_file = self.config_file_path
        with open(config_file) as f:
            config = json.load(f)
        file_types = config['file_types']
        file_types_dict = json.loads(file_types)
        file_types_list = file_types_dict.keys()
        #file_types = ['places']s
        # for file_type in file_types_list:
        #     fd = open(self.sql_file_path, 'r')
        #     sql = fd.read()
        #     fd.close()

        #     sql = sql.replace('{iam}', Variable.get('var-redshift-sts-role'))
        #     sql = sql.replace('{table}', file_type)
        #     sql = sql.replace('{output_bucket}', Variable.get('var-manifest-bucket'))
        #     sql = sql.replace('{output_path}', Variable.get('var-manifest-path'))

        #     self.log.info(f'Executing query: {sql}')

        #     redshift_hook.run(sql)

        def parallel_data_copy(file_type):

            fd = open(self.sql_file_path, 'r')
            sql = fd.read()
            fd.close()
            sql = sql.replace('{iam}', Variable.get('var-redshift-sts-role'))
            sql = sql.replace('{table}', file_type)
            sql = sql.replace('{output_bucket}', Variable.get('var-manifest-bucket'))
            sql = sql.replace('{output_path}', Variable.get('var-manifest-path'))
            self.log.info(f'Executing query: {sql}')
            redshift_hook.run(sql)

        # Using multithreading to process the copy event data in paralell.
        with futures.ThreadPoolExecutor() as executor:
            result = executor.map(parallel_data_copy, file_types_list)





