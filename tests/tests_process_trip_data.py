import csv
import os
from unittest import TestCase, mock

from process_trip_data import process_trip_data, save_csv_file, get_zone_dict, main


class ProcessGeneralDataTest(TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.data_path = os.path.join(dir_path, 'trip_files')
        self.file_path = os.path.join(dir_path, 'trip_files/2016-03-14.trip')
        self.file_path_gz = os.path.join(dir_path, 'trip_files/2016-03-14.trip.gz')
        self.file_path_zip = os.path.join(dir_path, 'trip_files/2016-03-14.trip.zip')
        self.file_path_without_data = os.path.join(dir_path, 'trip_files/2016-nodata.trip')
        self.zone777_path = os.path.join(dir_path, 'trip_files/zone_dictionary.csv')

    def test_get_zone_dict(self):
        expected_dict = {'135': 'CERRO NAVIA', '56': 'RENCA', '522': 'PUDAHUEL', '137': 'CERRO NAVIA', '837': 'RENCA',
                         '136': 'CERRO NAVIA'}

        self.assertDictEqual(expected_dict, get_zone_dict(self.zone777_path))

    def test_process_trip_data(self):
        expected_dict = {'ÑUÑOA': {'RECOLETA': 1.31}, 'RECOLETA': {'SANTIAGO': 1.33},
                         'SANTIAGO': {'ÑUÑOA': 1.31, 'SANTIAGO': 1.23}, 'LA FLORIDA': {'LA FLORIDA': 1.34},
                         'CERRILLOS': {'MAIPU': 1.36}, 'MAIPU': {'CERRILLOS': 1.41}, 'LAS CONDES': {'SANTIAGO': 1.33}}

        self.assertDictEqual(expected_dict, process_trip_data(self.file_path))

    def test_process_tripl_data_correct_gz(self):
        expected_dict = {'ÑUÑOA': {'RECOLETA': 1.31}, 'RECOLETA': {'SANTIAGO': 1.33},
                         'SANTIAGO': {'ÑUÑOA': 1.31, 'SANTIAGO': 1.23}, 'LA FLORIDA': {'LA FLORIDA': 1.34},
                         'CERRILLOS': {'MAIPU': 1.36}, 'MAIPU': {'CERRILLOS': 1.41}, 'LAS CONDES': {'SANTIAGO': 1.33}}

        self.assertDictEqual(expected_dict, process_trip_data(self.file_path_gz))

    def test_process_trip_data_correct_zip(self):
        expected_dict = {'ÑUÑOA': {'RECOLETA': 1.31}, 'RECOLETA': {'SANTIAGO': 1.33},
                         'SANTIAGO': {'ÑUÑOA': 1.31, 'SANTIAGO': 1.23}, 'LA FLORIDA': {'LA FLORIDA': 1.34},
                         'CERRILLOS': {'MAIPU': 1.36}, 'MAIPU': {'CERRILLOS': 1.41}, 'LAS CONDES': {'SANTIAGO': 1.33}}

        self.assertDictEqual(expected_dict, process_trip_data(self.file_path_zip))

    def test_process_trip_data_nodata(self):
        with self.assertRaises(StopIteration) as cm:
            process_trip_data(self.file_path_without_data)
            self.assertEqual(cm.exception.code, 1)

    def test_save_csv_file(self):
        data = [self.file_path]
        output = self.data_path
        output_filename = 'test'
        save_csv_file(data, output, output_filename)
        expected = [['Fecha', 'Comuna_origen', 'Comuna_destino', 'N°_viajes_expandidos'],
                    ['2016-03-14', 'ÑUÑOA', 'RECOLETA', '1.31'],
                    ['2016-03-14', 'RECOLETA', 'SANTIAGO', '1.33'],
                    ['2016-03-14', 'SANTIAGO', 'ÑUÑOA', '1.31'],
                    ['2016-03-14', 'SANTIAGO', 'SANTIAGO', '1.23'],
                    ['2016-03-14', 'LA FLORIDA', 'LA FLORIDA', '1.34'],
                    ['2016-03-14', 'CERRILLOS', 'MAIPU', '1.36'],
                    ['2016-03-14', 'MAIPU', 'CERRILLOS', '1.41'],
                    ['2016-03-14', 'LAS CONDES', 'SANTIAGO', '1.33']]
        with open(os.path.join(self.data_path, 'test.csv')) as test:
            reader = csv.reader(test)
            for row in expected:
                self.assertEqual(row, next(reader))

    @mock.patch('process_trip_data.config')
    @mock.patch('process_trip_data.save_csv_file')
    @mock.patch('process_trip_data.get_files')
    @mock.patch('process_trip_data.AWSSession')
    @mock.patch('process_trip_data.OUTPUT_PATH')
    @mock.patch('process_trip_data.INPUTS_PATH')
    @mock.patch('process_trip_data.DIR_PATH')
    def test_main(self, dir_path, input_path, output_path, aws_session, get_files,
                  save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [self.file_path]
        save_csv_file.side_effect = None
        main(['process_trip_data', 'input'])

    def tearDown(self):
        test = os.path.join(self.data_path, 'test.csv')
        if os.path.exists(test):
            os.remove(test)
