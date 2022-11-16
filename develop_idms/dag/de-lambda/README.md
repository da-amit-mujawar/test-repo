# *LAMBDA Function to trigger Airflow DAG when file arrives at S3*

- Code repo contains lambda function code which runs Airflow DAG when file is
  arrived at the S3 location specified as trigger location
- First you need to create a lambda function with trigger specified as S3 path
  and prefix and suffix values.
- Also, credentials are stored in secrets manager.
- Config is also sourced to pass airflow server details.
- You need to specify DAG name in code that needs to run once file arrived.
- ~~DAG arguments are also passed while calling DAG through API
  call.~~ [Configurable Arguments to be implemented]

# Deployment Steps

1. Upload **Lyr_Request.zip** into s3 location **s3:
   //idms-7933-prod-code/airflow-lambda-trigger**
2. Create a Lambda layer named **da-lyr-airflow-lambda-trigger-http-requests**
   and upload the Lyr-Request.zip
3. Create the lambda function named **da-airflow-lambda-trigger-dag**
4. Set the variable `instructions_bucket`  as appropriate.
    - Stage - `idms-2722-code`
    - Prod - `idms-7933-prod-code`
5. Set the variable `instructions_filename` as appropriate.
    - Stage - `airflow-lambda-trigger/lambda-dag-config.json`
    - Prod - `airflow-lambda-trigger/lambda-dag-config.json`
6. Make sure its on the same VPC, Subnet and Security Group as Airflow Instance

### To Add new Source
1. For the files under new bucket, add a s3 trigger on the lambda function **da-airflow-lambda-trigger-dag**
2. Modify lambda-dag-config.json and add a document under **files**
```
{
    "secretobjname": "Secret Object name",
    "secretname": "Secret Key Name on the Key Pair Value",
    "username": "UserName to call the DAG",
    "files": {
        "FileName": {
            "dagname": "Dag to be called",
            "paramters": []
        },
        "ultimate-new-mover/IDMS_UNM_Weekly.txt.gz": {
            "dagname": "build-premover-load-daily"
        }
    }
}

```
