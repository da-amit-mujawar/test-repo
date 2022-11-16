import boto3
from io import StringIO
import os
import logging
import json
import datetime
import awswrangler as wr
from botocore.exceptions import ClientError


def save_dataframe(bucket_name, key, df, delimeter, **kwargs):
    csv_buffer = StringIO()
    include_header = kwargs.get("include_header", False)
    df.to_csv(csv_buffer, delimeter, index=False, header=include_header)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket_name, key).put(Body=csv_buffer.getvalue())


def delete_file(bucket_name, key):
    s3_client = boto3.client("s3")
    response = s3_client.delete_object(Bucket=bucket_name, Key=key)


def copy_file(bucket_name, copy_source, key):
    s3_client = boto3.client("s3")
    response = s3_client.copy_object(
        Bucket=bucket_name, CopySource=copy_source, Key=key
    )


def read_file(bucket_name, key):
    s3_client = boto3.client("s3")
    return (
        s3_client.get_object(Bucket=bucket_name, Key=key)["Body"].read().decode("utf-8")
    )


def move_file(src_bucket, src_prefix, dest_bucket, dest_prefix):
    """
    Copies multiple files with specified Prefix into destination S3 location with prefix.
    It only copies up to 1,000 files.
    The function doesn't support recursive copy of a folder.

    Example: Move all file starting with "a1" into s3://develop_idms-2722-playground/rohit_rajput/output_files/
        move_file("develop_idms-2722-playground", "rohit_rajput/input_files/a1", "develop_idms-2722-playground", "rohit_rajput/output_files/")
    """
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(
        Bucket=src_bucket, Prefix=src_prefix, MaxKeys=1000
    )
    for file in response["Contents"]:
        key = file["Key"]
        copy_source = {"Bucket": src_bucket, "Key": key}
        just_keyname = os.path.basename(key)
        copy_file(dest_bucket, copy_source, f"{dest_prefix}{just_keyname}")
        delete_file(src_bucket, key)


def getObjectList(bucketName, prefix_key, maxKeys):
    s3 = boto3.client("s3")
    content = s3.list_objects_v2(
        Bucket=bucketName, Prefix=prefix_key, MaxKeys=maxKeys
    ).get("Contents")
    result = []
    for c in content:
        result.append(c.get("Key"))
    return result


def fix_s3_objects_permissions(bucket, prefix):
    """Get a list of all keys in an S3 bucket."""
    client = boto3.client("s3")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    failures = []
    while_true = True
    while while_true:
        resp = client.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            # print(obj['Key'])
            try:
                set_acl(bucket, obj["Key"])
            except KeyError:
                while_true = False
            except Exception:
                print(f"error - {obj['Key']}")
                failures.append(obj["Key"])
                continue
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break


def set_acl(bucket, key):
    client = boto3.client("s3")
    client.put_object_acl(ACL="bucket-owner-full-control", Bucket=bucket, Key=key)


def get_account_canonical_id():
    client = boto3.client("s3")
    return client.list_buckets()["Owner"]["ID"]


def get_files_using_wr(s3path):
    # example: s3://myBucket/raw/client/Hist/2017/*/*/Tracking_*.zip
    return wr.s3.list_objects(s3path)


def create_presigned_url(bucket_name,object_name,expiration=3600):
        s3_client = boto3.client('s3',"us-east-1")
        try:
            response = s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': bucket_name,
                                                                'Key': object_name},
                                                                 ExpiresIn=expiration,
                                                                 HttpMethod='GET'
                                                                 )
                                                        
        except ClientError as e:
            logging.error(e)
            return None

        return response  