import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import List

import mock
from moto import mock_secretsmanager

import job_salesforce_to_s3
from common.salesforce import SalesforceObject, SalesforceTable


def raise_exception(*args):
    raise Exception("Breaking Program")


def raise_connection_reset_error(*args):
    raise ConnectionResetError("Check Connection")


# pylint: disable=R0904
# pylint: disable=too-many-instance-attributes
class TestSalesforceToS3(unittest.TestCase):
    @mock_secretsmanager
    @mock.patch(
        "common.secret_manager.SecretManagerClass.get_secret_from_secret_manager",
        return_value={
            "sf_api_username": "test",
            "sf_api_password": "test",
            "sf_security_token": "cscbsdhcbj",
            "sf_api_version": "29.0",
            "sf_host": "https://salesforce.login.com",
        },
    )
    @mock.patch(
        "job_salesforce_to_s3.getResolvedOptions",
        return_value={"sf_api_secret_name": "salesforceTest"},
    )
    @mock.patch("job_salesforce_to_s3.SalesforceBulk")
    @mock.patch("job_salesforce_to_s3.Salesforce")
    @mock.patch("job_salesforce_to_s3.logger")
    def setUp(
        self,
        logger_mocked,
        salesforce_connection_mocked,
        bulk_salesforce_mocked,
        get_resolved_options_mock,
        get_secret_from_secret_manager_mock,
    ):
        self.salesforce_table: SalesforceTable = {
            "object_name": "testObject",
            "excluded_fields": ["test2"],
        }

        self.secret_name = "salesforceTest"
        self.secret_string = (
            '{"sf_api_username":"test",'
            '"sf_api_password":"test",'
            '"sf_security_token":"cscbsdhcbj",'
            '"sf_api_version":"29.0",'
            '"sf_host":"https://salesforce.login.com"}'
        )

        salesforce_connection_mocked().session_id = "testSessionId"
        salesforce_connection_mocked().sf_instance = "testHost"
        self.main_object = job_salesforce_to_s3.MainClass()
        get_secret_from_secret_manager_mock.assert_called()

        get_resolved_options_mock.assert_called()

        salesforce_connection_mocked.assert_called_with(
            username="test",
            password="test",
            security_token="cscbsdhcbj",
            version="29.0",
        )
        self.salesforce_connection = salesforce_connection_mocked()
        logger_mocked.info.assert_called()

        bulk_salesforce_mocked.assert_called_with(
            sessionId="testSessionId", host="testHost", API_version="29.0"
        )
        self.bulk_salesforce = bulk_salesforce_mocked()

        self.salesforce_connection.testObject.describe.return_value = {
            "fields": [
                {"name": "test"},
                {"name": "test1"},
                {"name": "test2"},
                {"name": "CreatedDate"},
                {"name": "LastModifiedDate"},
            ]
        }
        salesforce_object = SalesforceObject()
        self.regular_table_list = salesforce_object.regular_table_list
        self.non_pk_chunking_table_list = salesforce_object.non_pk_chunking_table_list

    def tearDown(self):
        self.bulk_salesforce.reset_mock()
        self.salesforce_connection.reset_mock()

    @mock.patch(
        "job_salesforce_to_s3.ProcessPoolExecutor", side_effect=ThreadPoolExecutor
    )
    @mock.patch(
        "job_salesforce_to_s3.MainClass.move_salesforce_to_s3",
        return_value={"status": True, "object_name": "test"},
    )
    def test_multi_process(
        self, mock_move_salesforce_to_s3, process_pool_executor_mock
    ):
        return_value = self.main_object.multi_process()
        self.assertTrue(mock_move_salesforce_to_s3.called)
        self.assertEqual(
            return_value,
            [
                {"status": True, "object_name": "test"}
                for _ in self.regular_table_list + self.non_pk_chunking_table_list
            ],
        )
        process_pool_executor_mock.assert_called()

    @mock.patch(
        "job_salesforce_to_s3.MainClass.move_salesforce_to_s3", return_value=True
    )
    def test_multi_process_with_empty_lists(self, mock_move_salesforce_to_s3):
        self.main_object.regular_table_list = []
        self.main_object.non_pk_chunking_table_list = []
        with self.assertRaises(RuntimeError):
            self.main_object.multi_process()
        self.assertFalse(mock_move_salesforce_to_s3.called)

    def test_remove_unwanted_fields_with_empty_list(self):
        result = self.main_object.remove_unwanted_fields([], [])
        expected_result: List[str] = []
        self.assertEqual(result, expected_result)

    def test_remove_unwanted_fields_with_equal_arrays(self):
        result = self.main_object.remove_unwanted_fields(
            ["name1", "name2"], ["name1", "name2"]
        )
        expected_result: List[str] = []
        self.assertEqual(result, expected_result)

    def test_remove_unwanted_fields_with_one_value(self):
        result = self.main_object.remove_unwanted_fields(["name1", "name2"], ["name1"])
        expected_result: List[str] = ["name2"]
        self.assertEqual(result, expected_result)

    def test_remove_unwanted_fields_with_duplicates_in_initial_list(self):
        result = self.main_object.remove_unwanted_fields(
            ["name1", "name1", "name2"], ["name1"]
        )
        expected_result: List[str] = ["name2"]
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
