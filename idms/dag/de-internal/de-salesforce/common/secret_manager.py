import json
import sys

import boto3
from botocore.exceptions import ClientError

from common.logging import LoggerSetup
from common.s3 import handle_client_error_and_exit

logger = LoggerSetup(logger_name=str(__file__)).get_logger()


class SecretManagerClass:
    def __init__(self, secretName: str, regionName: str) -> None:
        self.secretName = secretName
        self.regionName = regionName

    def get_secret_from_secret_manager(self) -> dict:
        '''
        The function getSecretFromSecretManager() initializes the SecretManagerClass class
        object when called with two params "secretName" and "regionName" is which secret
        is stored and will return a dictionary object.
        :return -> dict: A dictionary object containing secret value as key-value pairs
        '''
        getSecretValueResponse = {"SecretString": "{}"}
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager", region_name=self.regionName
        )
        try:
            getSecretValueResponse = client.get_secret_value(SecretId=self.secretName)
        except ClientError as err:
            handle_client_error_and_exit(err)
        else:
            if "SecretString" in getSecretValueResponse:
                logger.info("Fetched secrets from AWS SecretManager")
            else:
                raise ValueError("SecretString not found in reponse")
        return json.loads(getSecretValueResponse["SecretString"])
