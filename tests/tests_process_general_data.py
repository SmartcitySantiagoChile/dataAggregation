import csv
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
        with self.assertRaises(StopIteration) as cm:
            process_general_data(self.file_path_without_data)
            self.assertEqual(cm.exception.code, 1)

    def test_save_csv_file(self):
        data = [['2018-10-01', '5930344']]
        output = self.data_path
        output_filename = 'test'
        save_csv_file(data, output, output_filename)
        with open(os.path.join(output, output_filename + '.csv'), 'r', newline='\n', encoding='UTF-8') as outfile:
            r = csv.reader(outfile)
            first = next(r)
            self.assertEqual(['Fecha', 'Transacciones'], first)
            second = next(r)
            self.assertEqual(['2018-10-01', '5930344'], second)

    @mock.patch('process_general_data.config')
    @mock.patch('process_general_data.save_csv_file')
    @mock.patch('process_general_data.process_general_data')
    @mock.patch('process_general_data.get_files')
    @mock.patch('process_general_data.AWSSession')
    @mock.patch('process_general_data.OUTPUT_PATH')
    @mock.patch('process_general_data.INPUTS_PATH')
    @mock.patch('process_general_data.DIR_PATH')
    def test_main(self, dir_path, input_path, output_path, aws_session, get_files,
                  p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        main(['process_general_data', 'input'])

    def tearDown(self):
        test = os.path.join(self.data_path, 'test.csv')
        if os.path.exists(test):
            os.remove(test)
