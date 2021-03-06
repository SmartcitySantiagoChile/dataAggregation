import csv
import gzip
import logging
import os
from unittest import TestCase, mock

from process_general_data import process_general_data, save_csv_file, main


class ProcessGeneralDataTest(TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.data_path = os.path.join(dir_path, 'general_files')
        self.file_path = os.path.join(dir_path, 'general_files/2018-10-01.general')
        self.file_path_gz = os.path.join(dir_path, 'general_files/2018-10-01.general.gz')
        self.file_path_zip = os.path.join(dir_path, 'general_files/2018-10-01.general.zip')
        self.file_path_without_data = os.path.join(dir_path, 'general_files/2018-nodata.general')
        self.file_path_empty_zip = os.path.join(dir_path, 'general_files/2019-10-nodata.general.zip')
        self.file_path_empty_gz = os.path.join(dir_path, 'general_files/2019-10-nodata.general.gz')
        logging.disable(logging.CRITICAL)

    def test_process_general_data_correct(self):
        expected = ['2018-10-01', '5930344']
        self.assertEqual(expected, process_general_data(self.file_path))

    def test_process_general_data_correct_gz(self):
        expected = ['2018-10-01', '5930344']
        self.assertEqual(expected, process_general_data(self.file_path_gz))

    def test_process_general_data_correct_zip(self):
        expected = ['2018-10-01', '5930344']
        self.assertEqual(expected, process_general_data(self.file_path_zip))

    def test_process_general_data_nodata(self):
        self.assertIsNone(process_general_data(self.file_path_without_data))

    def test_process__trip_data_empty_zip(self):
        self.assertIsNone(process_general_data(self.file_path_empty_zip))

    def test_process__trip_data_empty_gz(self):
        self.assertIsNone(process_general_data(self.file_path_empty_gz))

    def test_save_csv_file(self):
        data = [['2018-10-01', '5930344']]
        output = self.data_path
        output_filename = 'test'
        save_csv_file(data, output, output_filename)
        with gzip.open(os.path.join(self.data_path, output_filename) + '.gz', 'rt') as outfile:
            r = csv.reader(outfile)
            first = next(r)
            self.assertEqual(['Fecha', 'Transacciones'], first)
            second = next(r)
            self.assertEqual(['2018-10-01', '5930344'], second)

    @mock.patch('process_general_data.config')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.OUTPUT_PATH')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main(self, dir_path, input_path, output_path, get_files,
                  p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        main(['process_general_data', 'input'])

    @mock.patch('process_general_data.send_data_to_s3')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main_with_s3(self, dir_path, input_path, get_files,
                          p_data, save_csv_file, send_s3):
        send_s3.side_effect = mock.MagicMock()
        dir_path.return_value = 'dir'
        input_path.return_value = 'input'
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        main(['process_general_data', 'input', '--send-to-s3'])

    @mock.patch('process_general_data.config')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.OUTPUT_PATH')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main_with_lower_bound_only(self, dir_path, input_path, output_path, get_files,
                                        p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_general_data', 'input', '--lower-bound', '2020-10-10'])

    @mock.patch('process_general_data.config')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.OUTPUT_PATH')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main_with_upper_bound_only(self, dir_path, input_path, output_path, get_files,
                                        p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_general_data', 'input', '--upper-bound', '2020-10-10'])

    @mock.patch('process_general_data.config')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.OUTPUT_PATH')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main_with_upper_bound_lower(self, dir_path, input_path, output_path, get_files,
                                         p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_general_data', 'input', '--lower-bound', '2021-10-10', '--upper-bound', '2020-10-10'])

    @mock.patch('process_general_data.config')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.OUTPUT_PATH')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main_with_lower_and_upper_bound(self, dir_path, input_path, output_path, get_files,
                                             p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = ['2018-10-01', '5930344']
        save_csv_file.side_effect = None
        main(['process_general_data', 'input', '--lower-bound', '2018-10-01', '--upper-bound', '2020-01-01'])

    @mock.patch('process_general_data.config')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.OUTPUT_PATH')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main_with_lower_and_upper_bound_pass(self, dir_path, input_path, output_path, get_files,
                                                  p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = ['2018-10-01', '5930344']
        save_csv_file.side_effect = None
        main(['process_general_data', 'input', '--lower-bound', '2019-10-01', '--upper-bound', '2020-01-01'])

    def tearDown(self):
        test_gz = os.path.join(self.data_path, 'test.gz')
        if os.path.exists(test_gz):
            os.remove(test_gz)
