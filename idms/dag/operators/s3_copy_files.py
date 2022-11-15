from typing import Optional, Union
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class S3CopyObjectOperator(BaseOperator):
    """
    Creates a copy of an object or objects that is (are) already stored in S3.
    Note: the S3 connection used here needs to have access to both
    source and destination bucket/key.
    :param source_bucket_key: The key of the source object. (templated)
        It can be either full s3:// style url or relative path from root level.
        When it's specified as a full s3:// url, please omit source_bucket_name.
    :type source_bucket_key: str
    :param dest_bucket_key: The key of the object to copy to. (templated)
        The convention to specify `dest_bucket_key` is the same as `source_bucket_key`.
    :type dest_bucket_key: str
    :param source_bucket_name: Name of the S3 bucket where the source object is in. (templated)
        It should be omitted when `source_bucket_key` is provided as a full s3:// url.
    :type source_bucket_name: str
    :param dest_bucket_name: Name of the S3 bucket to where the object is copied. (templated)
        It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
    :type dest_bucket_name: str
    :param source_version_id: Version ID of the source object (OPTIONAL)
    :type source_version_id: str
    :param aws_conn_id: Connection id of the S3 connection to use
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:
        - False: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    :type acl_policy: str
    """

    template_fields = ('source_bucket_key', 'dest_bucket_key', 'source_bucket_name', 'dest_bucket_name')

    @apply_defaults
    def __init__(
        self,
        *,
        source_bucket_key: Optional[str] = None,
        dest_bucket_key: Optional[str] = None,
        source_bucket_name: Optional[str] = None,
        dest_bucket_name: Optional[str] = None,
        source_version_id: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        acl_policy: Optional[str] = None,
        dest_file_extension: Optional[str] = '',
        bulk_copy: Optional[bool] = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.source_bucket_key = source_bucket_key
        self.dest_bucket_key = dest_bucket_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.source_version_id = source_version_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.acl_policy = acl_policy
        self.dest_file_extension = dest_file_extension
        self.bulk_copy = bulk_copy

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3bucket=s3_hook.get_bucket(self.source_bucket_name)
        dest_file_ext=''
        if self.dest_file_extension:
            dest_file_ext = f'.{self.dest_file_extension}'
        # for copying all file in the a folder to another
        if self.bulk_copy:
            for obj in s3bucket.objects.filter(Prefix=self.source_bucket_key):
                if obj.key[-1:] != '/':
                    key = obj.key.split('/')[-1]
                    source_file = f's3://{self.source_bucket_name}/{self.source_bucket_key}{key}'
                    #dest_file = f's3://{self.dest_bucket_name}/{self.dest_bucket_key}{key}.{self.dest_file_extension}'
                    dest_file = f's3://{self.dest_bucket_name}/{self.dest_bucket_key}{key}{dest_file_ext}'
                    print(source_file)
                    print(dest_file)
                    s3_hook.copy_object(source_file, dest_file)

        # for copying one single file to another folder
        else:
            s3_hook.copy_object(f's3://{self.source_bucket_key}', f's3://{self.dest_bucket_key}')
