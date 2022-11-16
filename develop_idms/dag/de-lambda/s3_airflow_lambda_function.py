# --------------------------------------------------------------------------------
# Name       Date     Version Purpose
# -------- ---------- ------- --------------------------------------------------
# Ram      08/04/2021   0.0    Modified Lambda Function to read data from JSON file
#
# --------------------------------------------------------------------------------
import base64
import re
import json
from datetime import datetime
import boto3
import requests
from botocore.exceptions import ClientError
import os
from requests.auth import HTTPBasicAuth
from io import BytesIO
import zipfile
from urllib.parse import unquote


def getsecret(ps_secret_name):
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    secret = ""
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=ps_secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return secret


def replace_variables(ps_json_data):
    ls_json_data = str(ps_json_data)
    ls_yyyy = datetime.strftime(datetime.today(), "%Y")
    ls_yy = datetime.strftime(datetime.today(), "%y")
    ls_mm = datetime.strftime(datetime.today(), "%m")
    ls_dd = datetime.strftime(datetime.today(), "%d")
    ls_json_data = ls_json_data.replace('{yyyy}', ls_yyyy) \
        .replace('{yy}', ls_yy) \
        .replace('{mm}', ls_mm) \
        .replace('{dd}', ls_dd)
    return ls_json_data


# Added logic to unzip all axle-donorbase-raw-sources : IDMS-1675
def unzipAndUpload(bucket, zip_key):
    s3_resource = boto3.resource('s3')
    zip_key = zip_key.replace('+', ' ')
    zip_key = unquote(zip_key)
    print("zipped file key: ", zip_key)
    zip_obj = s3_resource.Object(bucket_name=bucket, key=zip_key)
    buffer = BytesIO(zip_obj.get()["Body"].read())
    filename_key = zip_key[0:zip_key.rindex(".")] + '/'
    z = zipfile.ZipFile(buffer)

    print("List of files available in zip: ", z.namelist())

    for filename in z.namelist():
        filename_key_split = filename_key.split("/")
        filename_split = filename.split("/")
        filename_key_obj = filename_key + filename
        if (len(filename_key_split) > 1 and
                len(filename_split) > 1 and
                filename_key_split[len(filename_key_split) - 2] == filename_split[0]):
            filename_key_obj = filename_key + filename[filename.find("/") + 1:]
        try:
            s3_resource.meta.client.upload_fileobj(
                z.open(filename),
                Bucket=bucket,
                Key=f'{filename_key_obj}')
        except:
            print("Error: Unable to unzip file > " + filename_key_obj)

def lambda_handler(event, context):
    instructions_bucket = os.environ['instructions_bucket']
    instructions_filename = "airflow-lambda-trigger/lambda-dag-config.json"
    s3 = boto3.resource("s3")
    obj = s3.Object(instructions_bucket, instructions_filename)
    job_instructions = obj.get()["Body"].read().decode("utf-8")
    job_instructions = replace_variables(job_instructions)
    instructions = json.loads(job_instructions)
    ls_secret_obj_name = instructions.get('secretobjname')
    ls_secret_name = instructions.get('secretname')
    # Retrieve secret from Secrets Manager
    ls_secret = getsecret(ls_secret_obj_name)
    lj_secret = json.loads(json.loads(ls_secret)[ls_secret_name])
    ls_password = lj_secret.get('password')
    ls_server_name = lj_secret.get('servername')
    ls_username = lj_secret.get('username')
    la_files_list = instructions.get('files')
    ls_bucket_name = event.get('Records')[0].get('s3').get('bucket').get('name')
    ls_data_file_key = event.get('Records')[0].get('s3').get('object').get('key')
    status_code = 405
    try:
        for key in la_files_list:
            if re.match(r"%s" % (key), ls_data_file_key):
                lj_cur_file = la_files_list[key]
                ls_dag_list = lj_cur_file.get('dagname')
                print('ls_dag_list', ls_dag_list)
                for i in range(0, len(ls_dag_list)):
                    ls_dag_name = ls_dag_list[i]
                    print('ls_dag_name', ls_dag_name)
                    print('testing33')
                    url_trigger = 'http://' + ls_server_name + '/api/v1/dags/' + ls_dag_name + '/dagRuns'
                    ld_headers = {'Content-type': 'application/json'}
                    ld_data = {"input": "lambda"}
                    ld_data["bucket_name"] = ls_bucket_name
                    ld_data["file_key"] = ls_data_file_key
                    print("URL:[{0}], DAG:[{1}], Bucket: [{2}], File:[{3}]".format(url_trigger, ls_dag_name,
                                                                                   ls_bucket_name,
                                                                                   ls_data_file_key))
                    # POST Request to trigger DAG
                    response = requests.post(url_trigger,
                                             auth=HTTPBasicAuth(ls_username, ls_password),
                                             json={'conf': ld_data}, headers=ld_headers)
                    print(response.json())
                    status_code = response.status_code
    except Exception as e:
        print("Error: Dag Name not Found for the file {0}.".format(ls_data_file_key))
        print("Error:", e)
        raise

    # Added logic to unzip all axle-donorbase-raw-sources : IDMS-1675
    try:
        if ls_bucket_name == 'axle-donorbase-raw-sources' and (
                ls_data_file_key.endswith(".zip") or ls_data_file_key.endswith(".ZIP")):
            unzipAndUpload(ls_bucket_name, ls_data_file_key)
            print("Successfully uploaded files from zip : " + ls_data_file_key)
    except:
        print("Error: Unable to unzip file: " + ls_data_file_key)
    return status_code

# evntData = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1',
#                          'eventTime': '2021-07-27T20:30:18.368Z',
#                          'eventName': 'ObjectCreated:CompleteMultipartUpload', 'userIdentity': {
#         'principalId': 'AWS:AROATUQ6QQ4RNAHGEBI7J:Saravanan.RamaLingam@data-axle.com'},
#                          'requestParameters': {'sourceIPAddress': '199.125.14.2'},
#                          'responseElements': {'x-amz-request-id': 'S5E0B6RAC8260Z2T',
#                                               'x-amz-id-2': 'gXIJLTS1Y2CykiHvnbnTdwFxHv7eAit7OYGW6oyY6UweRVhYKjmBm6a1fLEl5yIcShJNdw9C3apd3sVbRHLVqibw+Fiib4Gi'},
#                          's3': {'s3SchemaVersion': '1.0',
#                                 'configurationId': '66d335c0-f6ea-4ba9-979e-f1992a8aeefd',
#                                 'bucket': {'name': 'develop_idms-2722-playground',
#                                            'ownerIdentity': {'principalId': 'A8PA8YVL3IXGW'},
#                                            'arn': 'arn:aws:s3:::develop_idms-2722-playground'},
#                                 'object': {'key': 'ram/develop_idms-1454/SanFranData-2021-08-02.csv',
#                                            'size': 55872074,
#                                            'eTag': '1ee601ad23ff1e05f012961420ae9493-4',
#                                            'sequencer': '0061006CD003A07291'}}}]}
#
# (lambda_handler(evntData, "-"))