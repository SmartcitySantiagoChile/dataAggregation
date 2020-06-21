import boto3
import botocore
from botocore.exceptions import ClientError
from decouple import config
from datetime import datetime


class AWSSession:
    """
    Class to interact wit Amazon Web Service (AWS) API through boto3 library
    """

    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'))
        self.bucket_name = config('EARLY_TRANSACTION_BUCKET_NAME')

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