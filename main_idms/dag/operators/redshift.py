from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.hooks.jdbc_hook import JdbcHook
import json
import glob


class RedshiftOperator(BaseOperator):
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

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        # Define your operators params (with defaults) here
        redshift_conn_id="",
        aws_credentials_id="",
        sql_file_path="",
        config_file_path="",
        databaseid=0,
        table="",
        s3_bucket="",
        s3_key="",
        region="",
        *args,
        **kwargs,
    ):
        super(RedshiftOperator, self).__init__(*args, **kwargs)
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
        self.autocommit = True

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        current_date = datetime.now()
        config_file = self.config_file_path
        with open(config_file) as f:
            config = json.load(f)

        for sql_file in sorted(glob.glob(self.sql_file_path)):
            fd = open(sql_file, "r")
            sql = fd.read()
            fd.close()

            sql = sql.replace("{iam}", Variable.get("var-password-redshift-iam-role"))
            sql = sql.replace("{kmskey}", Variable.get("var-password-kms-key"))

            build_info = Variable.get(
                "var-db-" + str(self.databaseid),
                deserialize_json=True,
                default_var=None,
            )
            if build_info:
                ### IDMS 1663: get all items as strings (cb)
                # maintable_name = build_info.get("maintable_name", "")
                # sql = sql.replace("{build_id}", str(build_info["build_id"]))
                # sql = sql.replace("{build}", build_info["build"])
                # if maintable_name:
                #     sql = sql.replace("{maintable_name}", maintable_name)
                for item in build_info:
                    sql = sql.replace("{"+f"{item}"+"}", str(build_info[item]))
                ### IDMS 1663: end

            for item in config:
                sql = sql.replace("{"+f"{item}"+"}", str(config[item]))

            s3_bucket_name = Variable.get("var-s3-bucket-names", deserialize_json=True)

            for bucket in s3_bucket_name:
                sql = sql.replace("{"+f"{bucket}"+"}", s3_bucket_name[bucket])

            sql = sql.replace("{dd}", current_date.strftime("%d"))
            sql = sql.replace("{mm}", current_date.strftime("%m"))
            sql = sql.replace("{yyyy}", current_date.strftime("%Y"))
            today = datetime.now()
            yesterday = datetime.now() - timedelta(1)
            sql = sql.replace("{yesterday}", datetime.strftime(yesterday, "%Y%m%d"))
            sql = sql.replace("{today}", datetime.strftime(today, "%Y%m%d"))
            sql = sql.replace(
                "{emailoversight-prefix}", Variable.get("var-emailoversight-prefix")
            )

            self.log.info(f"Executing query: {sql}")
            redshift_hook.run(sql, autocommit=True)
