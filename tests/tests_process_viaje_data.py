import csv
import gzip
import logging
import os
from unittest import TestCase, mock

from process_viaje_data import process_viaje_data, get_commune_for_extra_location, save_csv_file, main


class ProcessViajeDataTest(TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.data_path = os.path.join(dir_path, 'viaje_files')
        self.file_path = os.path.join(self.data_path, '2020-03-01.viaje')
        self.file_path_gz = os.path.join(self.data_path, '2020-03-01.viaje.gz')
        self.file_path_zip = os.path.join(self.data_path, '2020-03-01.viaje.zip')
        self.file_path_without_data = os.path.join(self.data_path, '2020-nodata.viaje')
        self.file_path_empty_zip = os.path.join(self.data_path, '2020-nodata.viaje.zip')
        self.file_path_empty_gz = os.path.join(self.data_path, '2020-nodata.viaje.gz')
        logging.disable(logging.CRITICAL)

    def test_get_commune_for_extra_location_start_station(self):
        extra_location_row = ['43771714', '1', '1', '-', '0', '1', '507.0000', '8.4500', '8979.2012', '0.0000',
                              'ESTACION SAN BERNARDO', 'ESTACION LO ESPEJO', '-', '-', '434', '411',
                              '2020-03-01 15:43:08',
                              '2020-03-01 15:51:35', '05 - MEDIODIA DOMINGO', '05 - MEDIODIA DOMINGO', 'DOMINGO',
                              '15:30:00',
                              '-', '1.7604', '2020-03-01 15:47:21', '05 - MEDIODIA DOMINGO', '15:30:00', 'DOMINGO',
                              '8.4500',
                              '0.0000', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
                              '-',
                              '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', 'METROTREN', '-', '-',
                              '-',
                              '-', '-', '-', '-', '-', '-', '-', '-', 'ESTACION SAN BERNARDO', '-', '-', '-',
                              '2020-03-01 15:43:08', '-', '-', '-', '434', '-', '-', '-', 'ESTACION LO ESPEJO', '-',
                              '-',
                              '-', '2020-03-01 15:51:35', '-', '-', '-', '411', '-', '-', '-', 'METROTREN', '-', '-',
                              '-',
                              '-', '-', '-', '-', '-', '8.4500', 'M40M', 'OTROS', '-']

        expected_start_commune = "San Bernardo"
        expected_end_commune = "Lo Espejo"
        errors, start_commune, end_commune = get_commune_for_extra_location(extra_location_row, None,
                                                                            None)
        self.assertIsNotNone(errors)
        self.assertEqual(expected_start_commune, start_commune)
        self.assertEqual(expected_end_commune, end_commune)

    def test_get_commune_for_extra_location_end_station(self):
        extra_location_row = ['43771714', '1', '1', '-', '0', '1', '507.0000', '8.4500', '8979.2012', '0.0000',
                              'ESTACION SAN BERNARDO', 'ESTACION LO ESPEJO', '-', '-', '434', '411',
                              '2020-03-01 15:43:08',
                              '2020-03-01 15:51:35', '05 - MEDIODIA DOMINGO', '05 - MEDIODIA DOMINGO', 'DOMINGO',
                              '15:30:00',
                              '-', '1.7604', '2020-03-01 15:47:21', '05 - MEDIODIA DOMINGO', '15:30:00', 'DOMINGO',
                              '8.4500',
                              '0.0000', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
                              '-',
                              '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', 'METROTREN', '-', '-',
                              '-',
                              '-', '-', '-', '-', '-', '-', '-', '-', 'ESTACION SAN BERNARDO', '-', '-', '-',
                              '2020-03-01 15:43:08', '-', '-', '-', '434', '-', '-', '-', 'ESTACION LO ESPEJO', '-',
                              '-',
                              '-', '2020-03-01 15:51:35', '-', '-', '-', '411', '-', '-', '-', 'METROTREN', '-', '-',
                              '-',
                              '-', '-', '-', '-', '-', '8.4500', 'M40M', 'OTROS', '-']

        expected_start_commune = "San Bernardo"
        expected_end_commune = "Lo Espejo"
        errors, start_commune, end_commune = get_commune_for_extra_location(extra_location_row, "San Bernardo",
                                                                            None)
        self.assertEqual(expected_start_commune, start_commune)
        self.assertEqual(expected_end_commune, end_commune)

    def test_get_commune_for_extra_location_no_commune(self):
        row = ['43771714', '1', '1', '-', '0', '1', '507.0000', '8.4500', '8979.2012', '0.0000',
               'EXAMPLE_WITH_NO_COMMUNE', 'EXAMPLE_WITH_NO_COMMUNE', '-', '-', '434', '411', '2020-03-01 15:43:08',
               '2020-03-01 15:51:35', '05 - MEDIODIA DOMINGO', '05 - MEDIODIA DOMINGO', 'DOMINGO', '15:30:00',
               '-', '1.7604', '2020-03-01 15:47:21', '05 - MEDIODIA DOMINGO', '15:30:00', 'DOMINGO', '8.4500',
               '0.0000', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
               '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', 'METROTREN', '-', '-', '-',
               '-', '-', '-', '-', '-', '-', '-', '-', 'ESTACION SAN BERNARDO', '-', '-', '-',
               '2020-03-01 15:43:08', '-', '-', '-', '434', '-', '-', '-', 'ESTACION LO ESPEJO', '-', '-',
               '-', '2020-03-01 15:51:35', '-', '-', '-', '411', '-', '-', '-', 'METROTREN', '-', '-', '-',
               '-', '-', '-', '-', '-', '8.4500', 'M40M', 'OTROS', '-']

        errors, start_commune, end_commune = get_commune_for_extra_location(row, None,
                                                                            None)
        expected_error = {'Example_With_No_Commune'}
        self.assertEqual(expected_error, errors)
        self.assertIsNone(start_commune)
        self.assertIsNone(end_commune)

    def test_process_viaje_data(self):
        expected_dict = {'San Miguel': {'Santiago': 1.5236},
                         'Santiago': {'San Miguel': 1.4524},
                         'Recoleta': {'Recoleta': 1.5408},
                         'Ñuñoa': {'Ñuñoa': 1.4085}}
        self.assertEqual(expected_dict, process_viaje_data(self.file_path)[0])

    def test_process_viaje_data_correct_gz(self):
        expected_dict = {'San Miguel': {'Santiago': 1.5236},
                         'Santiago': {'San Miguel': 1.4524},
                         'Recoleta': {'Recoleta': 1.5408},
                         'Ñuñoa': {'Ñuñoa': 1.4085}}
        self.assertDictEqual(expected_dict, process_viaje_data(self.file_path_gz)[0])

    def test_process_viaje_data_correct_zip(self):
        expected_dict = {'San Miguel': {'Santiago': 1.5236},
                         'Santiago': {'San Miguel': 1.4524},
                         'Recoleta': {'Recoleta': 1.5408},
                         'Ñuñoa': {'Ñuñoa': 1.4085}}
        self.assertDictEqual(expected_dict, process_viaje_data(self.file_path_zip)[0])

    def test_process_viaje_data_nodata(self):
        self.assertIsNone(process_viaje_data(self.file_path_without_data)[0])

    def test_process_viaje_data_empty_zip(self):
        self.assertIsNone(process_viaje_data(self.file_path_empty_zip)[0])

    def test_process_viaje_data_empty_gz(self):
        self.assertIsNone(process_viaje_data(self.file_path_empty_gz)[0])

    def test_save_csv_file(self):
        data = [self.file_path, self.file_path_empty_gz, self.file_path_empty_zip, self.file_path_without_data]
        output = self.data_path
        output_filename = 'test'
        save_csv_file(data, output, output_filename)
        expected = [['Fecha', 'Comuna_origen', 'Comuna_destino', 'N°_viajes_expandidos'],
                    ['2020-03-01', 'San Miguel', 'Santiago', '1.5236'],
                    ['2020-03-01', 'Santiago', 'San Miguel', '1.4524'],
                    ['2020-03-01', 'Recoleta', 'Recoleta', '1.5408'],
                    ['2020-03-01', 'Ñuñoa', 'Ñuñoa', '1.4085']]
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
                    ['2020-03-01', 'San Miguel', 'Santiago', '1.5236'],
                    ['2020-03-01', 'Santiago', 'San Miguel', '1.4524'],
                    ['2020-03-01', 'Recoleta', 'Recoleta', '1.5408']]
        with gzip.open(os.path.join(self.data_path, output_filename) + '.gz', 'rt') as outfile:
            reader = csv.reader(outfile)
            for row in expected:
                self.assertEqual(row, next(reader))

    @mock.patch('process_viaje_data.config')
    @mock.patch('process_viaje_data.save_csv_file')
    @mock.patch('process_viaje_data.get_files')
    @mock.patch('process_viaje_data.OUTPUT_PATH')
    @mock.patch('process_viaje_data.INPUTS_PATH')
    @mock.patch('process_viaje_data.DIR_PATH')
    def test_main(self, dir_path, input_path, output_path, get_files,
                  save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [self.file_path]
        save_csv_file.side_effect = None
        main(['process_viaje_data', 'input'])

    @mock.patch('process_viaje_data.send_data_to_s3')
    @mock.patch('process_viaje_data.save_csv_file')
    @mock.patch('process_viaje_data.process_viaje_data')
    @mock.patch('process_viaje_data.get_files')
    @mock.patch('process_viaje_data.INPUTS_PATH')
    @mock.patch('process_viaje_data.DIR_PATH')
    def test_main_with_s3(self, dir_path, input_path, get_files,
                          p_data, save_csv_file, send_s3):
        send_s3.side_effect = mock.MagicMock()
        dir_path.return_value = 'dir'
        input_path.return_value = 'input'
        get_files.return_value = [os.path.join(self.data_path, '2020-03-01.viaje')]
        save_csv_file.side_effect = None
        main(['process_viaje_data', 'input', '--send-to-s3'])

    @mock.patch('process_viaje_data.config')
    @mock.patch('process_viaje_data.save_csv_file')
    @mock.patch('process_viaje_data.process_viaje_data')
    @mock.patch('process_viaje_data.get_files')
    @mock.patch('process_viaje_data.OUTPUT_PATH')
    @mock.patch('process_viaje_data.INPUTS_PATH')
    @mock.patch('process_viaje_data.DIR_PATH')
    def test_main_with_lower_bound_only(self, dir_path, input_path, output_path, get_files,
                                        p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2020-03-01.viaje')]
        p_data.return_value = [['2020-03-01', '420420']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_viaje_data', 'input', '--lower-bound', '2020-10-10'])

    @mock.patch('process_viaje_data.config')
    @mock.patch('process_viaje_data.save_csv_file')
    @mock.patch('process_viaje_data.process_viaje_data')
    @mock.patch('process_viaje_data.get_files')
    @mock.patch('process_viaje_data.OUTPUT_PATH')
    @mock.patch('process_viaje_data.INPUTS_PATH')
    @mock.patch('process_viaje_data.DIR_PATH')
    def test_main_with_upper_bound_only(self, dir_path, input_path, output_path, get_files,
                                        p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2020-03-01.viaje')]
        p_data.return_value = [['2020-03-01', '420420']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_viaje_data', 'input', '--upper-bound', '2020-10-10'])

    @mock.patch('process_viaje_data.config')
    @mock.patch('process_viaje_data.save_csv_file')
    @mock.patch('process_viaje_data.process_viaje_data')
    @mock.patch('process_viaje_data.get_files')
    @mock.patch('process_viaje_data.OUTPUT_PATH')
    @mock.patch('process_viaje_data.INPUTS_PATH')
    @mock.patch('process_viaje_data.DIR_PATH')
    def test_main_with_upper_bound_lower(self, dir_path, input_path, output_path, get_files,
                                         p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2020-03-01.viaje')]
        p_data.return_value = [['2020-03-01', '420420']]
        save_csv_file.side_effect = None
        with self.assertRaises(SystemExit) as cm:
            main(['process_viaje_data', 'input', '--lower-bound', '2021-10-10', '--upper-bound', '2020-10-10'])

    @mock.patch('process_viaje_data.config')
    @mock.patch('process_viaje_data.save_csv_file')
    @mock.patch('process_viaje_data.process_viaje_data')
    @mock.patch('process_viaje_data.get_files')
    @mock.patch('process_viaje_data.OUTPUT_PATH')
    @mock.patch('process_viaje_data.INPUTS_PATH')
    @mock.patch('process_viaje_data.DIR_PATH')
    def test_main_with_lower_and_upper_bound(self, dir_path, input_path, output_path, get_files,
                                             p_data, save_csv_file, config):
        dir_path.return_value = self.data_path
        input_path.return_value = self.data_path
        output_path.return_value = self.data_path
        get_files.return_value = [os.path.join(self.data_path, '2020-03-01.viaje')]
        p_data.return_value = ['2020-03-01', '5930344']
        save_csv_file.side_effect = None
        main(['process_viaje_data', 'input', '--lower-bound', '2019-10-01', '--upper-bound', '2020-01-01'])

    def tearDown(self):
        test_gz = os.path.join(self.data_path, 'test.gz')

        if os.path.exists(test_gz):
            os.remove(test_gz)
