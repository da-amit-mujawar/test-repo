import base64
import json

import requests
from airflow.exceptions import AirflowFailException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class DatabricksJobSensor(BaseSensorOperator):
    """ This is a sensor to monitor the job completion on the databricks side """

    @apply_defaults
    def __init__(
        self, domain="", token="", prevtaskid="", key="", *args, **kwargs
    ):
        super(DatabricksJobSensor, self).__init__(*args, **kwargs)
        self.domain = domain
        self.token = token
        self.prevtaskid = prevtaskid
        self.key = key

    def poke(self, context):
        task_instance = context["task_instance"]
        runid = task_instance.xcom_pull(task_ids=self.prevtaskid, key=self.key)
        TOKEN = self.token.encode("utf-8")

        endpoint = f"https://{self.domain}/api/2.0/jobs/runs/get"
        print(f"endpoint is {endpoint}")
        response = requests.get(
            endpoint,
            headers={
                "Authorization": b"Basic "
                + base64.standard_b64encode(b"token:" + TOKEN)
            },
            json={"run_id": runid},
        )

        print(response)
        if (
            response.status_code == 200
            and response.json()["state"]["life_cycle_state"] == "TERMINATED"
        ):
            result_state = response.json()["state"]["result_state"]
            if result_state == "SUCCESS":
                print(response.json())
                print(f"Job run status for the runid {runid} is {result_state}")
                return True
            else:
                raise AirflowFailException(
                    f"The Databricks job with {runid} failed."
                )
        else:
            # print("Error launching cluster: %s" % (response.json()))
            return False
