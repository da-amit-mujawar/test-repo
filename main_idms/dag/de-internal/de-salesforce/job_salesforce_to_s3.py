import sys
from concurrent.futures import ProcessPoolExecutor
from typing import List

from simple_salesforce import Salesforce, exceptions
from salesforce_bulk import SalesforceBulk
from awsglue.utils import getResolvedOptions
from common.logging import LoggerSetup
from common.secret_manager import SecretManagerClass
from common.salesforce import SalesforceObject, SalesforceTable


logger = LoggerSetup(logger_name=str(__file__)).get_logger()


class MainClass:
    # pylint: disable=too-many-instance-attributes
    def __init__(self):

        self.salesforce_object = SalesforceObject()
        self.regular_table_list: List[
            SalesforceTable
        ] = self.salesforce_object.regular_table_list
        self.non_pk_chunking_table_list: List[
            SalesforceTable
        ] = self.salesforce_object.non_pk_chunking_table_list

        args = getResolvedOptions(sys.argv, ["sf_api_secret_name"])
        salesforce_secret_object = SecretManagerClass(
            args["sf_api_secret_name"], "us-east-1"
        )
        salesforce_credential = (
            salesforce_secret_object.get_secret_from_secret_manager()
        )

        if "sf_domain" in salesforce_credential.keys():
            self.salesforce_connection = Salesforce(
                username=salesforce_credential["sf_api_username"],
                password=salesforce_credential["sf_api_password"],
                organizationId=salesforce_credential["sf_org_id"],
                domain=salesforce_credential["sf_domain"],
                security_token=salesforce_credential["sf_security_token"],
                version=salesforce_credential["sf_api_version"],
            )
        elif "test" not in salesforce_credential["sf_host"]:
            self.salesforce_connection = Salesforce(
                username=salesforce_credential["sf_api_username"],
                password=salesforce_credential["sf_api_password"],
                security_token=salesforce_credential["sf_security_token"],
                version=salesforce_credential["sf_api_version"],
            )
        else:
            raise Exception(
                "EnvironmentError: Please check the environment in sf_api_secret_name"
            )
        logger.info(
            f"Authenticated to Salesforce host {salesforce_credential['sf_host']} successfully"
        )
        self.bulk_salesforce = SalesforceBulk(
            sessionId=self.salesforce_connection.session_id,
            host=self.salesforce_connection.sf_instance,
            API_version=salesforce_credential["sf_api_version"],
        )
        logger.info("Salesforce Bulk API connected successfully")

    @staticmethod
    def check_return_values(return_value_list: list) -> None:
        for value in return_value_list:
            if not value["status"]:
                raise RuntimeError("Error occured please check logs")

    @staticmethod
    def remove_unwanted_fields(
        field_names: List[str], excluded_fields: List[str]
    ) -> List[str]:
        return [fields for fields in field_names if fields not in excluded_fields]

    def multi_process(self) -> list:
        return_value_list = []
        if (
            len(self.regular_table_list) == 0
            and len(self.non_pk_chunking_table_list) == 0
        ):
            raise RuntimeError("No objects found! Please mention objects.")
        with ProcessPoolExecutor(max_workers=3) as executor:
            for table in self.non_pk_chunking_table_list:
                return_value = executor.submit(
                    self.move_salesforce_to_s3,
                    table,
                    pk_chunking=False,
                )
                return_value_list.append(return_value.result())
            for table in self.regular_table_list:
                return_value = executor.submit(self.move_salesforce_to_s3, table)
                return_value_list.append(return_value.result())
        return return_value_list

    def move_salesforce_to_s3(
        self,
        table: SalesforceTable,
        retry_mechanism_flag: bool = False,
        pk_chunking: bool = True,
    ) -> dict:
        # pylint: disable=too-many-branches
        # pylint: disable=too-many-statements
        error_flag = False
        object_name = table["object_name"]
        try:
            description_dictionary = getattr(
                self.salesforce_connection, object_name
            ).describe()
            field_names = [field["name"] for field in description_dictionary["fields"]]
            field_names = self.remove_unwanted_fields(
                field_names, table["excluded_fields"]
            )
        except exceptions.SalesforceResourceNotFound as err:
            logger.critical(
                f"Please check if {object_name} exists in the given salesforce environment"
            )
            logger.error(err, exc_info=True)
            error_flag = True
        except Exception as err:
            logger.error(err, exc_info=True)
            error_flag = True
        logger.info(f"**{object_name}** : {field_names}")
        if error_flag:
            return {"status": False, "object": object_name}
        return {"status": True, "object": object_name}

    def runner(self) -> None:
        return_value_list = self.multi_process()
        logger.info(f"Returned status of objects: {return_value_list}")
        self.check_return_values(return_value_list)


if __name__ == "__main__":
    try:
        main_object = MainClass()
        main_object.runner()
    except Exception as err:
        logger.error(err, exc_info=True)
        sys.exit(err)
