
import sys
import datetime
from dateutil.parser import parse

from botocore.exceptions import ClientError
import boto3

from common.logging import LoggerSetup

logger = LoggerSetup(logger_name=str(__file__)).get_logger()


def handle_client_error_and_exit(err: ClientError) -> None:
    '''
    This function holds the generic boto3 error codes and defining error logs accordingly.
    :param err: A ClientError type object from boto3 library to be compared
    to the standard error codes and define error message accordingly to be 
    logged into output console.
    '''
    if err.response["Error"]["Code"] == "IllegalLocationConstraintException":
        errorMessage = (
            "You are attempting to access a bucket from "
            "a different region than where the bucket exists."
        )
    elif err.response["Error"]["Code"] == "NoSuchBucket":
        errorMessage = "The specified bucket does not exist!"
    elif err.response["Error"]["Code"] == "NoSuchKey":
        errorMessage = "No such key in bucket found - returning empty"
    elif err.response["Error"]["Code"] == "ResourceNotFoundException":
        errorMessage = "The requested resource was not found"
    elif err.response["Error"]["Code"] == "InvalidRequestException":
        errorMessage = f"The request was invalid due to: {err}"
    elif err.response["Error"]["Code"] == "InvalidParameterException":
        errorMessage = f"The request had invalid params: {err}"
    elif err.response["Error"]["Code"] == "404":
        errorMessage = "Error occured with status code 404"
    elif err.response["Error"]["Code"] == "AccessDenied":
        errorMessage = "Access Denied"
    if "errorMessage" in locals():
        logger.error(errorMessage, exc_info=True)
        sys.exit(err)
    else:
        raise Exception(err) 


def is_date(string: str, fuzzy: bool=False) -> bool:
    """
    Return whether the string can be interpreted as a date.
    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False

class S3Objects:
    def __init__(self, bucket_name: str, directory_name: str, file_format: str) -> None:
        self.bucket_name = bucket_name
        self.directory_name = directory_name
        self.file_format = file_format

    
    @staticmethod
    def fetch_date_from_s3_file_path(filepath: str) -> datetime.date:
        """
        Return the date if exists in the s3 filepath string.
        :param filepath: str, string from which fate has to be extracted out.
        """
        date_patterned_string = filepath[filepath.rindex("_")+1:]
        if is_date(date_patterned_string, True):
            date_from_filename = date_patterned_string[
                : date_patterned_string.index(".")
            ]
            return datetime.datetime.strptime(date_from_filename, "%Y-%m-%d").date()
        else:
            raise(f"The s3 filepath {filepath} doesn't contain any date string.")


    def get_latest_file_path(self) -> str:
        last_added_file_path = ""
        get_last_modified_datetime = lambda s3_object: int(
            s3_object["LastModified"].strftime("%Y%m%d%H%M%S")
        )
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects")
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.directory_name,
        )
        for page in page_iterator:
            try:
                if "Contents" in page:
                    sorted_files_by_last_modified = [
                        s3_object["Key"]
                        for s3_object in sorted(
                            page["Contents"], key=get_last_modified_datetime
                        )
                    ]
                    logger.debug(
                        f"Sorted s3 files in {self.directory_name} : {sorted_files_by_last_modified}"
                    )
                    if last_added_file_path == "" or self.fetch_date_from_s3_file_path(
                        sorted_files_by_last_modified[-1]
                    ) > self.fetch_date_from_s3_file_path(last_added_file_path):
                        last_added_file_path = sorted_files_by_last_modified[-1]
            except ClientError as err:
                handle_client_error_and_exit(err)
        logger.info(f"Last uploaded file: {last_added_file_path}")
        return last_added_file_path
        

    def last_extraction_date(self, key: str, default_extract_date: str) -> str:
        latest_s3_file_path = self.get_latest_file_path()
        logger.debug(latest_s3_file_path)
        if latest_s3_file_path == "":
            formatted_date = default_extract_date
        else:
            formatted_date = str(self.fetch_date_from_s3_file_path(latest_s3_file_path))
        logger.info(f"Last extracted date for {key} was {formatted_date}")
        return formatted_date


