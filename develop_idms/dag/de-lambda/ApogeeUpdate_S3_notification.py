import json
import boto3
from datetime import date
from datetime import datetime
import boto3
import requests
from botocore.exceptions import ClientError
from io import BytesIO
import zipfile
from urllib.parse import unquote


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
    try:
        client = boto3.client('s3')

        ### current date
        today = date.today()

        print("Today date is: ", today)

        # update s3 bucket to 'develop_idms-2722-axle-raw-customer-sources' for development testing
        response2 = client.list_objects_v2(
            Bucket='axle-raw-customer-sources',
            Prefix='apogee'
        )

        res = ""

        if response2['Contents']:
            for j in response2['Contents']:
                datetime_element = j['LastModified']
                date_element = datetime_element.date()
                if (date_element == today):
                    res = res + j['Key'].replace("apogee/", "") + "\n"

        print('res', res)

        if res != '':  ##if msg is not empty then only send mail

            client = boto3.client('sns')

            snsArn = 'arn:aws:sns:us-east-1:479134617933:ApogeeUpdate_S3_notification'
            message = "Date:  " + str(today) + "\n" + "File Names -: " + "\n" + str(res)

            response = client.publish(
                TopicArn=snsArn,
                Message=message,
                Subject='Apogee File Drop Notification'
            )
    except Exception as e:
        print("Error:", e)

    # IDMS-2408: Auto unzipping for axle-raw-customer-sources
    ls_bucket_name = event.get('Records')[0].get('s3').get('bucket').get('name')
    ls_data_file_key = event.get('Records')[0].get('s3').get('object').get('key')

    if ls_bucket_name == 'axle-raw-customer-sources' and (ls_data_file_key.endswith(".zip") or ls_data_file_key.endswith(".ZIP")):
        unzipAndUpload(ls_bucket_name, ls_data_file_key)
        print("Successfully uploaded files from zip : " + ls_data_file_key)
