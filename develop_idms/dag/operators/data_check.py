from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.utils.decorators import apply_defaults
import json


class DataCheckOperator(BaseOperator):
    """
    Data quality check
    Parameters: redshift_conn_id: Redshift connection ID
                config_file_path: config file location, used to get table names in redshift
                min_expected_count: the expected counts for talbes
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        # Define your operators params (with defaults) here
        redshift_conn_id="",
        config_file_path="",
        min_expected_count="",
        *args,
        **kwargs,
    ):

        super(DataCheckOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.config_file_path = config_file_path
        self.min_expected_count = min_expected_count

    def execute(self, context):
        self.log.info("DataCheckOperator is checking for credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # redshift_hook = JdbcHook(jdbc_conn_id=self.redshift_conn_id)

        self.log.info("Running Data Quality Check")

        config_file = self.config_file_path
        with open(config_file) as f:
            config = json.load(f)

        for (table, min_count) in self.min_expected_count:
            if table in config:
                table_name = config[table]
            else:
                table_name = table

            self.log.info(f"Table checking: {table_name}")
            sql = f"select count(*) from {table_name}"
            count = redshift_hook.get_first(sql)
            if count is not None:
                if count[0] > min_count:
                    self.log.info(
                        f"Data Check Passed: Table {table_name} Count {count[0]}. Minimum Expected Count: {min_count}."
                    )
                else:
                    raise ValueError(
                        f"Data Check Failed: Table {table_name} Count {count[0]}. Minimum Expected Count: {min_count}."
                    )
