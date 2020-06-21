import os
from unittest import TestCase
from utils import get_files


class TestUtils(TestCase):

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.data_path = os.path.join(dir_path, 'general_files')

    def test_get_files(self):
        print(get_files('general', self.data_path))
