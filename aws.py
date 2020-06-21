import urllib

import boto3
import botocore
from botocore.exceptions import ClientError
from decouple import config


class AWSSession:
    """
    Class to interact wit Amazon Web Service (AWS) API through boto3 library
    """

    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'))

    def check_bucket_exists(self, bucket_name):
        s3 = self.session.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                raise ValueError("Private Bucket. Forbidden Access!")
            elif error_code == 404:
                return False

    def _build_url(self, key, bucket_name):
        return ''.join(['https://s3.amazonaws.com/', bucket_name, '/', urllib.parse.quote(key)])

    def send_file_to_bucket(self, file_path, file_key, bucket_name):
        s3 = self.session.resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(file_path, file_key)

        return self._build_url(file_key, bucket_name)
