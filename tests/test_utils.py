import os
from datetime import datetime
from unittest import TestCase, mock
import contextlib
from io import StringIO
from utils import get_files, send_data_to_s3, valid_date
import argparse


class TestUtils(TestCase):

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.data_path = os.path.join(dir_path, 'general_files')

    def test_get_files(self):
        expected_files = list(map(lambda x: os.path.join(self.data_path, x),
                                  ['2018-nodata.general', '2018-10-01.general', '2018-10-01.general.gz',
                                   '2018-10-01.general.zip']))
        self.assertEqual(expected_files, get_files('general', self.data_path))

    @mock.patch('utils.AWSSession')
    def test_send_to_s3_bucket_exist(self, awsession):
        temp_stdout = StringIO()
        with contextlib.redirect_stdout(temp_stdout):
            send_data_to_s3('path', 'bucket')
        output = temp_stdout.getvalue().strip()
        self.assertTrue(len(output) > 0)

    @mock.patch('utils.AWSSession')
    def test_send_to_s3_bucket_does_not_exist(self, awsession):
        bucket_exist = mock.MagicMock()
        bucket_exist.return_value = False
        awsession.return_value = mock.MagicMock(check_bucket_exists=bucket_exist)
        with self.assertRaises(SystemExit):
            send_data_to_s3('path', 'bucket')

    def test_valid_date(self):
        date = '2010-10-10'
        expected_date = datetime.strptime(date, "%Y-%m-%d")
        self.assertEqual(expected_date, valid_date(date))
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_date('1')
