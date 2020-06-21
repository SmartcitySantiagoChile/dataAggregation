import os
from unittest import TestCase

from utils import get_files


class TestUtils(TestCase):

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.data_path = os.path.join(dir_path, 'general_files')

    def test_get_files(self):
        expected_files = list(map(lambda x: os.path.join(self.data_path, x),
                                  ['2018-nodata.general', '2018-10-01.general', '2018-10-01.general.gz',
                                   '2018-10-01.general.zip']))
        self.assertEqual(expected_files, get_files('general', self.data_path))
