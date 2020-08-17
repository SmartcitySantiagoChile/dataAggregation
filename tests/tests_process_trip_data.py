import csv
import gzip
import logging
import os
from unittest import TestCase, mock

from process_trip_data import process_trip_data, save_csv_file, main, get_commune_for_metrotren_station


class ProcessTripDataTest(TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.data_path = os.path.join(dir_path, 'trip_files')
        self.file_path = os.path.join(dir_path, 'trip_files/2016-03-14.trip')
        self.file_path_gz = os.path.join(dir_path, 'trip_files/2016-03-14.trip.gz')
        self.file_path_zip = os.path.join(dir_path, 'trip_files/2016-03-14.trip.zip')
        self.file_path_without_data = os.path.join(dir_path, 'trip_files/2016-nodata.trip')
        self.file_path_empty_zip = os.path.join(dir_path, 'trip_files/2019-10-nodata.trip.zip')
        self.file_path_empty_gz = os.path.join(dir_path, 'trip_files/2019-10-nodata.trip.gz')
        logging.disable(logging.CRITICAL)

    def test_get_commune_for_metrotren_station(self):
        metrotren_row = ['2', '2.46', '2', '27.7000', '20807.70', '2330.00', '2020-03-01 10:53:17',
                         '2020-03-01 11:20:59', '21', '22', '25', '25', '1', '4', '-1', '-1', 'T232 00R', '-', '-', '-',
                         'L-30-13-25-OP', 'Estacion Alameda', '8', '-1', '562', '78', '5', '2020-03-01 10:53:17',
                         '2020-03-01 10:59:03', '-', '-', '2020-03-01 10:56:54', '2020-03-01 11:20:59', '-', '-', '562',
                         '560', '-', '-', '560', '78', '-', '-', 'L-30-13-25-OP', 'Estacion Nos', '-', '-',
                         'L-30-51-95-SN', 'Estacion Alameda', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '']

        is_metrotren, communes = get_commune_for_metrotren_station(metrotren_row)
        self.assertTrue(is_metrotren)

    def test_process_trip_data(self):
        expected_dict = {'Ñuñoa': {'Recoleta': 1.31}, 'Recoleta': {'Santiago': 1.33},
                         'Santiago': {'Ñuñoa': 1.31, 'Santiago': 1.23}, 'La Florida': {'La Florida': 1.34},
                         'Cerrillos': {'Maipú': 1.36}, 'Maipú': {'Cerrillos': 1.41}, 'Las Condes': {'Santiago': 1.33}}
        self.assertEqual(expected_dict, dict(process_trip_data(self.file_path)))

    def test_process_tripl_data_correct_gz(self):
        expected_dict = {'Ñuñoa': {'Recoleta': 1.31}, 'Recoleta': {'Santiago': 1.33},
                         'Santiago': {'Ñuñoa': 1.31, 'Santiago': 1.23}, 'La Florida': {'La Florida': 1.34},
                         'Cerrillos': {'Maipú': 1.36}, 'Maipú': {'Cerrillos': 1.41}, 'Las Condes': {'Santiago': 1.33}}
        self.assertDictEqual(expected_dict, process_trip_data(self.file_path_gz))

    def test_process_trip_data_correct_zip(self):
        expected_dict = {'Ñuñoa': {'Recoleta': 1.31}, 'Recoleta': {'Santiago': 1.33},
                         'Santiago': {'Ñuñoa': 1.31, 'Santiago': 1.23}, 'La Florida': {'La Florida': 1.34},
                         'Cerrillos': {'Maipú': 1.36}, 'Maipú': {'Cerrillos': 1.41}, 'Las Condes': {'Santiago': 1.33}}
        self.assertDictEqual(expected_dict, process_trip_data(self.file_path_zip))

    def test_process_trip_data_nodata(self):
        self.assertIsNone(process_trip_data(self.file_path_without_data))

    def test_process__trip_data_empty_zip(self):
        self.assertIsNone(process_trip_data(self.file_path_empty_zip))

    def test_process__trip_data_empty_gz(self):
        self.assertIsNone(process_trip_data(self.file_path_empty_gz))

    def test_save_csv_file(self):
        data = [self.file_path, self.file_path_empty_gz, self.file_path_empty_zip, self.file_path_without_data]
        output = self.data_path
        output_filename = 'test'
        save_csv_file(data, output, output_filename)
        expected = [['Fecha', 'Comuna_origen', 'Comuna_destino', 'N°_viajes_expandidos'],
                    ['2016-03-14', 'Ñuñoa', 'Recoleta', '1.31'],
                    ['2016-03-14', 'Recoleta', 'Santiago', '1.33'],
                    ['2016-03-14', 'Santiago', 'Ñuñoa', '1.31'],
                    ['2016-03-14', 'Santiago', 'Santiago', '1.23'],
                    ['2016-03-14', 'La Florida', 'La Florida', '1.34'],
                    ['2016-03-14', 'Cerrillos', 'Maipú', '1.36'],
                    ['2016-03-14', 'Maipú', 'Cerrillos', '1.41'],
                    ['2016-03-14', 'Las Condes', 'Santiago', '1.33']]
        with gzip.open(os.path.join(self.data_path, output_filename) + '.gz', 'rt') as outfile:
            reader = csv.reader(outfile)
            for row in expected:
                self.assertEqual(row, next(reader))

    def test_save_multiple_csv_file(self):
        data = [self.file_path]
        output = self.data_path
        output_filename = 'test'
        save_csv_file(data, output, output_filename)
        expected = [['Fecha', 'Comuna_origen', 'Comuna_destino', 'N°_viajes_expandidos'],
                    ['2016-03-14', 'Ñuñoa', 'Recoleta', '1.31'],
                    ['2016-03-14', 'Recoleta', 'Santiago', '1.33'],
                    ['2016-03-14', 'Santiago', 'Ñuñoa', '1.31'],
                    ['2016-03-14', 'Santiago', 'Santiago', '1.23'],
                    ['2016-03-14', 'La Florida', 'La Florida', '1.34'],
                    ['2016-03-14', 'Cerrillos', 'Maipú', '1.36'],
                    ['2016-03-14', 'Maipú', 'Cerrillos', '1.41'],
                    ['2016-03-14', 'Las Condes', 'Santiago', '1.33']]
        with gzip.open(os.path.join(self.data_path, output_filename) + '.gz', 'rt') as outfile:
            reader = csv.reader(outfile)
            for row in expected:
                self.assertEqual(row, next(reader))

    @mock.patch('process_trip_data.config')
    @mock.patch('process_trip_data.save_csv_file')
    @mock.patch('process_trip_data.get_files')
    @mock.patch('process_trip_data.OUTPUT_PATH')
    @mock.patch('process_trip_data.INPUTS_PATH')
    @mock.patch('process_trip_data.DIR_PATH')
    def test_main(self, dir_path, input_path, output_path, get_files,
                  save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [self.file_path]
        save_csv_file.side_effect = None
        main(['process_trip_data', 'input'])

    @mock.patch('process_trip_data.send_data_to_s3')
    @mock.patch('process_trip_data.save_csv_file')
    @mock.patch('process_trip_data.process_trip_data')
    @mock.patch('process_trip_data.get_files')
    @mock.patch('process_trip_data.INPUTS_PATH')
    @mock.patch('process_trip_data.DIR_PATH')
    def test_main_with_s3(self, dir_path, input_path, get_files,
                          p_data, save_csv_file, send_s3):
        send_s3.side_effect = mock.MagicMock()
        dir_path.return_value = 'dir'
        input_path.return_value = 'input'
        get_files.return_value = [os.path.join(self.data_path, '2016-03-14.general')]
        save_csv_file.side_effect = None
        main(['process_trip_data', 'input', '--send-to-s3'])

    @mock.patch('process_trip_data.config')
    @mock.patch('process_trip_data.save_csv_file')
    @mock.patch('process_trip_data.process_trip_data')
    @mock.patch('process_trip_data.get_files')
    @mock.patch('process_trip_data.OUTPUT_PATH')
    @mock.patch('process_trip_data.INPUTS_PATH')
    @mock.patch('process_trip_data.DIR_PATH')
    def test_main_with_lower_bound_only(self, dir_path, input_path, output_path, get_files,
                                        p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_trip_data', 'input', '--lower-bound', '2020-10-10'])

    @mock.patch('process_trip_data.config')
    @mock.patch('process_trip_data.save_csv_file')
    @mock.patch('process_trip_data.process_trip_data')
    @mock.patch('process_trip_data.get_files')
    @mock.patch('process_trip_data.OUTPUT_PATH')
    @mock.patch('process_trip_data.INPUTS_PATH')
    @mock.patch('process_trip_data.DIR_PATH')
    def test_main_with_upper_bound_only(self, dir_path, input_path, output_path, get_files,
                                        p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_trip_data', 'input', '--upper-bound', '2020-10-10'])

    @mock.patch('process_trip_data.config')
    @mock.patch('process_trip_data.save_csv_file')
    @mock.patch('process_trip_data.process_trip_data')
    @mock.patch('process_trip_data.get_files')
    @mock.patch('process_trip_data.OUTPUT_PATH')
    @mock.patch('process_trip_data.INPUTS_PATH')
    @mock.patch('process_trip_data.DIR_PATH')
    def test_main_with_upper_bound_lower(self, dir_path, input_path, output_path, get_files,
                                         p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = [['2018-10-01', '5930344']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_trip_data', 'input', '--lower-bound', '2021-10-10', '--upper-bound', '2020-10-10'])

    @mock.patch('process_trip_data.config')
    @mock.patch('process_trip_data.save_csv_file')
    @mock.patch('process_trip_data.process_trip_data')
    @mock.patch('process_trip_data.get_files')
    @mock.patch('process_trip_data.OUTPUT_PATH')
    @mock.patch('process_trip_data.INPUTS_PATH')
    @mock.patch('process_trip_data.DIR_PATH')
    def test_main_with_lower_and_upper_bound(self, dir_path, input_path, output_path, get_files,
                                             p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2018-10-01.general')]
        p_data.return_value = ['2018-10-01', '5930344']
        save_csv_file.side_effect = None
        main(['process_trip_data', 'input', '--lower-bound', '2019-10-01', '--upper-bound', '2020-01-01'])

    def tearDown(self):
        test_gz = os.path.join(self.data_path, 'test.gz')

        if os.path.exists(test_gz):
            os.remove(test_gz)
