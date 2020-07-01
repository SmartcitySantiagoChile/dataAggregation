import argparse
import glob
import gzip
import io
import os
import zipfile
from datetime import datetime

from aws import AWSSession


def is_gzipfile(file_path):
    with gzip.open(file_path) as file_obj:
        try:
            file_obj.read(1)
            return True
        except IOError:
            return False


def get_file_object(datafile):
    """
    :return: file object
    """
    if zipfile.is_zipfile(datafile):
        zip_file_obj = zipfile.ZipFile(datafile)
        # it assumes that zip file has only one file
        file_name = zip_file_obj.namelist()[0]
        file_obj_0 = zip_file_obj.open(file_name, 'r')
        file_obj = io.TextIOWrapper(file_obj_0, encoding='latin-1')
    elif is_gzipfile(datafile):
        file_obj = gzip.open(datafile, str('rt'), encoding='latin-1')
    else:
        file_obj = io.open(datafile, str('r'), encoding='latin-1')

    return file_obj


def get_files(file_type, path):
    types = ('*{}'.format(file_type), '*{0}.gz'.format(file_type), '*{0}.zip'.format(file_type))
    files = []
    for file in types:
        files.extend(glob.glob(os.path.join(path, file)))
    files.sort(key=lambda x: ''.join(os.path.basename(x)).split(".")[0])
    return files


def send_data_to_s3(path, bucket):
    aws_session = AWSSession()
    if not aws_session.check_bucket_exists(bucket):
        print('Bucket \'{0}\' does not exist'.format(bucket))
        exit(1)
    filename = ''.join(path.split('/')[-1]).split(".")[0] + '.gz'
    print('{0}: uploading file {1}'.format(datetime.now().replace(microsecond=0), path))
    aws_session.send_file_to_bucket(path, filename, bucket)
    print('{0}: finished load of file {1}'.format(datetime.now().replace(microsecond=0), path))


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)
