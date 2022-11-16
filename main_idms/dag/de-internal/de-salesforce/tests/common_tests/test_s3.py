import unittest
import json
from datetime import datetime
from time import sleep

import boto3
import mock
from botocore.exceptions import ClientError
from moto import mock_s3

import common.s3 as mainPackage


# pylint: disable=R0904
# pylint: disable=too-many-instance-attributes
class TestS3(unittest.TestCase):
    @mock_s3
    def setUp(self):
        self.number_of_files_mocked = 3
        self.number_of_files_mocked_with_date = 70
        self.dataset = "testDataset"
        self.default_extract_date = "2016-12-31"
        self.date_object = datetime(2020, 5, 17)
        self.response = {"Error": {"Code": "test"}}
        self.directory_name = "test/"
        self.bucket_name = "test"
        self.bucket_name_date_testing = "dateTest"
        self.file_format = "txt"
        self.s3_object_test = mainPackage.S3Objects(
            bucket_name=self.bucket_name,
            directory_name=self.directory_name,
            file_format=self.file_format,
        )
        self.s3_object_test_date_testing = mainPackage.S3Objects(
            bucket_name=self.bucket_name_date_testing,
            directory_name=self.directory_name,
            file_format=self.file_format,
        )

    def setupFixturesTemporary(self):
        sample_data_txt = (
            "Lorem ipsum dolor sit amet, consectetur "
            "adipiscing elit, sed do eiusmod tempor incididunt "
            "ut labore et dolore magna aliqua. Ut enim ad minim "
            "veniam, quis nostrud exercitation ullamco laboris nisi ut "
            "aliquip ex ea commodo consequat. "
            "Duis aute irure dolor in reprehenderit in "
            "voluptate velit esse cillum dolore eu "
            "fugiat nulla pariatur. Excepteur sint occaecat "
            "cupidatat non proident, sunt in culpa qui officia "
            "deserunt mollit anim id est laborum."
        )
        sample_data_csv = (
            "testCol1,testCol2\n"
            "testValue1,testValue2\n"
        )
        boto_s3_client = boto3.client("s3", region_name="us-east-1")
        boto_s3_client.create_bucket(Bucket=self.bucket_name)
        boto_s3_client.create_bucket(Bucket=self.bucket_name_date_testing)
        for index in range(0, self.number_of_files_mocked):
            boto_s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.directory_name + f"testFile{index}.txt",
                Body=sample_data_txt,
            )
        boto_s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.directory_name + "emptyFile.txt",
            Body="",
        )
        s3_resource = boto3.resource("s3")
        s3_resource.Object(self.bucket_name, "check.json").put(
            Body=(bytes(json.dumps({"a": 1}).encode("UTF-8")))
        )
        s3_resource.Object(self.bucket_name, "check.csv").put(Body=sample_data_csv)
        for index in range(0, self.number_of_files_mocked_with_date):
            self.date_object = datetime(2020, ((5 + (index // 30)) % 12), 1 + index % 30)
            boto_s3_client.put_object(
                Bucket=self.bucket_name_date_testing,
                Key=self.directory_name
                + f"testFile{index}_{str(self.date_object.date())}.txt",
                Body=sample_data_txt,
            )
        sleep(10)
        boto_s3_client.put_object(
            Bucket=self.bucket_name_date_testing,
            Key=self.directory_name + "testFile56126_2021-01-01.txt",
            Body=sample_data_txt,
        )

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_illegal_location_constraint_exception(
        self, logger_mocked, sys_exit_mocked
    ):
        self.response = {"Error": {"Code": "IllegalLocationConstraintException"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = (
            "You are attempting to access a bucket from "
            "a different region than where the bucket exists."
        )
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_exit_mocked.exit.assert_called()
        sys_exit_mocked.exit.assert_called_with(self)

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_no_such_bucket(self, logger_mocked, sys_mocked):
        self.response = {"Error": {"Code": "NoSuchBucket"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = "The specified bucket does not exist!"
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_mocked.exit.assert_called()
        sys_mocked.exit.assert_called_with(self)

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_no_such_key(self, logger_mocked, sys_mocked):
        self.response = {"Error": {"Code": "NoSuchKey"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = "No such key in bucket found - returning empty"
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_mocked.exit.assert_called()
        sys_mocked.exit.assert_called_with(self)

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_resource_not_found_exception(
        self, logger_mocked, sys_mocked
    ):
        self.response = {"Error": {"Code": "ResourceNotFoundException"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = "The requested resource was not found"
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_mocked.exit.assert_called()
        sys_mocked.exit.assert_called_with(self)

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_invalid_request_exception(
        self, logger_mocked, sys_mocked
    ):
        self.response = {"Error": {"Code": "InvalidRequestException"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = f"The request was invalid due to: {self}"
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_mocked.exit.assert_called()
        sys_mocked.exit.assert_called_with(self)

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_invalid_parameter_exception(
        self, logger_mocked, sys_mocked
    ):
        self.response = {"Error": {"Code": "InvalidParameterException"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = f"The request had invalid params: {self}"
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_mocked.exit.assert_called()
        sys_mocked.exit.assert_called_with(self)

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_404(self, logger_mocked, sys_mocked):
        self.response = {"Error": {"Code": "404"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = "Error occured with status code 404"
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_mocked.exit.assert_called()
        sys_mocked.exit.assert_called_with(self)

    @mock.patch("common.s3.sys")
    @mock.patch("common.s3.logger")
    def test_handle_client_error_and_exit_with_access_denied(self, logger_mocked, sys_mocked):
        self.response = {"Error": {"Code": "AccessDenied"}}
        mainPackage.handle_client_error_and_exit(err=self)
        validation_string = "Access Denied"
        logger_mocked.error.assert_called()
        logger_mocked.error.assert_called_with(validation_string, exc_info=True)
        sys_mocked.exit.assert_called()
        sys_mocked.exit.assert_called_with(self)

    def test_handle_client_error_and_exit_with_raise_err(self):
        self.response = {"Error": {"Code": "testing_wrong_code"}}
        with self.assertRaises(Exception):
            mainPackage.handle_client_error_and_exit(err=self)



    @mock_s3
    @mock.patch("common.s3.logger")
    def test_latest_file(self, logger_mocked):
        self.setupFixturesTemporary()
        return_value = self.s3_object_test.get_latest_file_path()
        self.assertEqual(return_value, f"test/testFile{self.number_of_files_mocked-1}.txt")
        logger_mocked.info.assert_called_with(f"Last uploaded file: {return_value}")

    @mock_s3
    @mock.patch("common.s3.logger")
    def test_latest_file_with_latest_file_check(self, logger_mocked):
        self.setupFixturesTemporary()
        return_value = self.s3_object_test_date_testing.get_latest_file_path()
        self.assertEqual(return_value, "test/testFile56126_2021-01-01.txt")
        logger_mocked.info.assert_called_with(f"Last uploaded file: {return_value}")

    @mock_s3
    def test_latest_file_with_error_check(self):
        with self.assertRaises(ClientError):
            self.s3_object_test.get_latest_file_path()

    def test_is_date_for_date_patterned_string(self):
        path_string = "2020-09-28.json"
        response_value = mainPackage.is_date(path_string, True)
        self.assertEqual(True, response_value)

    def test_is_date_for_non_date_patterned_string(self):
        path_string = "rid1.json"
        response_value = mainPackage.is_date(path_string, True)
        self.assertEqual(False, response_value)

    @mock.patch("common.s3.is_date", return_value=True)
    def test_fetch_date_from_s3_file_path(self, is_date_mocked):
        file_path_string = (
            "s3://datalake.staging.guidion.io/data-source/"
            "salesforce/case/processed files/jid750070000004OaaAAE"
            "_bid751070000004hY9AAI_rid1_2020-09-28.json"
        )
        expected_date = "2020-09-28"
        response_value = str(self.s3_object_test.fetch_date_from_s3_file_path(file_path_string))
        is_date_mocked.assert_called()
        self.assertEqual(expected_date, response_value)

    @mock_s3
    @mock.patch(
        "common.s3.S3Objects.get_latest_file_path",
        return_value="data-source/salesforce/testDataset/processed files/"
        "jid7501q000005JmZqAAK_bid7511q000005d5mjAAA_2020-09-10.json",
    )
    @mock.patch(
        "common.s3.S3Objects.fetch_date_from_s3_file_path",
        return_value="2020-09-10",
    )
    @mock.patch("common.s3.logger")
    def test_last_extraction_date(
        self,
        logger_mocked,
        s3_object_fetch_date_from_s3_file_path_mocked,
        s3_object_get_latest_file_path_mocked,
    ):
        self.setupFixturesTemporary()
        return_value = self.s3_object_test_date_testing.last_extraction_date(
            self.dataset, self.default_extract_date
        )
        self.assertEqual(return_value, "2020-09-10")
        s3_object_get_latest_file_path_mocked.assert_called()
        s3_object_fetch_date_from_s3_file_path_mocked.assert_called()
        logger_mocked.info.assert_called_with(
            f"Last extracted date for {self.dataset} was {return_value}"
        )

    @mock_s3
    @mock.patch(
        "common.s3.S3Objects.get_latest_file_path",
        return_value="",
    )
    @mock.patch("common.s3.logger")
    def test_last_extraction_date_for_empty_folder(
        self, logger_mocked, s3_object_get_latest_file_path_mocked
    ):
        self.setupFixturesTemporary()
        return_value = self.s3_object_test_date_testing.last_extraction_date(
            self.dataset, self.default_extract_date
        )
        self.assertEqual(return_value, "2016-12-31")
        s3_object_get_latest_file_path_mocked.assert_called()
        logger_mocked.info.assert_called_with(
            f"Last extracted date for {self.dataset} was {return_value}"
        )


if __name__ == "__main__":
    unittest.main()
