# -*- coding: utf8 -*-
import argparse
import csv
import gzip
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime

from decouple import config
from pyfiglet import Figlet

from aws import AWSSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
DATA_PATH = os.path.join(DIR_PATH, 'data')
INPUTS_PATH = os.path.join(DIR_PATH, 'inputs')
TEMPLATE_PATH = os.path.join(DIR_PATH, 'template')
OUTPUTS_PATH = os.path.join(DIR_PATH, 'outputs')


def check_available_days(aws_session, start_date, end_date):
    available_dates = aws_session.get_available_dates()
    dates_in_range = []
    for date in available_dates:
        if date < start_date:
            continue
        if date > end_date:
            break
        dates_in_range.append(date)
    return dates_in_range


def get_available_files(dates_in_range, aws_session, data_path):
    available_files = []
    for date in dates_in_range:
        filename = '{0}.4daytransactionbystop.gz'.format(date.strftime('%Y-%m-%d'))
        file_path = os.path.join(data_path, filename)
        if os.path.exists(file_path):
            logger.info('file {0} exists in local storage ... skip'.format(filename))
            available_files.append(file_path)
        else:
            aws_session.download_object_from_bucket(filename, file_path)
            available_files.append(file_path)
    return available_files


def get_output_dict(available_files):
    output = defaultdict(lambda: dict(info=dict(), dates=defaultdict(lambda: 0)))
    metro_stations = set()
    metrotren_stations = []
    for file_path in available_files:
        logger.info('reading file "{0}" ...'.format(os.path.basename(file_path)))
        with gzip.open(file_path, str('rt'), encoding='latin-1') as file_obj:
            # skip header
            file_obj.readline()
            for line in file_obj.readlines():
                values = line.split(';')
                stop_code = values[0].encode('latin-1').decode('utf-8')

                if stop_code == "-":
                    continue

                stop_name = values[5]

                if stop_name == "-":
                    stop_name = stop_code

                area = values[6]
                date = values[3]
                transactions = values[10]

                if values[8] == 'METRO':
                    stop_code = stop_code + values[9]
                    metro_stations.add(stop_code)

                output[stop_code]['info']['stop_name'] = stop_name
                output[stop_code]['info']['stop_code'] = stop_code
                output[stop_code]['info']['area'] = area.title()
                output[stop_code]['dates'][date] += int(transactions)

    return output, metro_stations, metrotren_stations


def add_location_to_stop_data(inputs_path, output, dates_in_range):
    with open(os.path.join(inputs_path, 'stop.csv'), encoding='latin-1') as csv_file_obj:
        spamreader = csv.reader(csv_file_obj, delimiter='|')
        next(spamreader)
        for row in spamreader:
            stop_code = row[4]
            stop_longitude = row[7]
            stop_latitude = row[8]

            output[stop_code]['info']['longitude'] = float(stop_longitude)
            output[stop_code]['info']['latitude'] = float(stop_latitude)
            if 'area' not in dict(output)[stop_code]['info']:
                output[stop_code]['info']['area'] = '-'
            if 'stop_name' not in dict(output)[stop_code]['info']:
                output[stop_code]['info']['stop_name'] = row[5]
            if 'stop_code' not in dict(output)[stop_code]['info']:
                output[stop_code]['info']['stop_code'] = stop_code
            if output[stop_code]['dates'] == {}:
                for date in dates_in_range:
                    output[stop_code]['dates'][date.strftime('%Y-%m-%d')] = 0

        return output


def add_location_to_metro_station_data(inputs_path, output, metro_stations, dates_in_range):
    with open(os.path.join(inputs_path, 'metro.csv')) as csv_metro_data:
        metro_data = csv.reader(csv_metro_data, delimiter=';')
        next(metro_data)
        for metro_station in metro_data:
            station_name = metro_station[7]
            line = metro_station[4]
            output[station_name + line]['info']['longitude'] = float(metro_station[2])
            output[station_name + line]['info']['latitude'] = float(metro_station[3])
            output[station_name + line]['info']['stop_name'] = "Estación {0}".format(station_name.title())
            output[station_name + line]['info']['stop_code'] = "Estación {0} {1}".format(station_name.title(), line)
            if station_name + line not in metro_stations:
                output[station_name + line]['info']['area'] = metro_station[1].title()
                for date in dates_in_range:
                    output[station_name + line]['dates'][date.strftime('%Y-%m-%d')] = 0
    return output


def add_location_to_metrotren_station_data(inputs_path, output, dates_in_range):
    with open(os.path.join(inputs_path, 'metrotren.geojson')) as metro:
        data = json.load(metro)
        for metrotren in data['features']:
            metro_station = metrotren['properties']['name']
            metro_latitude = metrotren['geometry']['coordinates'][0]
            metro_longitude = metrotren['geometry']['coordinates'][1]
            output[metro_station]['info']['longitude'] = float(metro_longitude)
            output[metro_station]['info']['latitude'] = float(metro_latitude)
            if not 'stop_name' in dict(output)[metro_station]['info']:
                output[metro_station]['info']['stop_name'] = metro_station
    return output


def create_csv_data(outputs_path, output_filename, output):
    with open(os.path.join(outputs_path, output_filename + '.csv'), 'w', newline='\n', encoding='latin-1') as outfile:
        csv_data = []
        w = csv.writer(outfile)
        w.writerow(['Fecha', 'Código de usuario', 'Código ts', 'Comuna', 'Latitud', 'Longitud', 'Subidas'])
        for data in dict(output):
            info = dict(output)[data]['info']
            longitude = '-'
            latitude = '-'
            area = '-'
            valid = True
            if 'longitude' in dict(output)[data]['info']:
                longitude = info['longitude']
            else:
                logger.warning("%s doesn't have longitude" % data)
                valid = False

            if 'latitude' in dict(output)[data]['info']:
                latitude = info['latitude']
            else:
                logger.warning("%s doesn't have latitude" % data)
                valid = False

            if 'area' in dict(output)[data]['info']:
                area = info['area']
            else:
                logger.warning("%s doesn't have area" % data)
                valid = False

            if 'stop_name' in dict(output)[data]['info']:
                stop_name = info['stop_name']
            else:
                logger.warning("%s doesn't have stop name" % data)
                valid = False

            if 'stop_code' in dict(output)[data]['info']:
                stop_code = info['stop_code']
            else:
                logger.warning("Warning: %s doesn't have stop name" % data)
                valid = False

            for date in dict(output)[data]['dates']:
                if valid:
                    data_row = [date + " 00:00:00", stop_name, stop_code, area, longitude, latitude,
                                dict(output)[data]['dates'][date]]
                    w.writerow(data_row)
                    csv_data.append(data_row)
    return csv_data


def write_info_to_kepler_file(template_path, outputs_path, output_filename, mapbox_key, csv_data):
    html_file = open(os.path.join(template_path, 'template.html'))
    html_data = html_file.read()
    html_file.close()
    with open(os.path.join(outputs_path, f"{output_filename}.html"), 'w') as output:
        new_html_data = html_data.replace("<MAPBOX_KEY>", mapbox_key).replace("<DATA>", str(csv_data))
        output.write(new_html_data)


def main(argv):
    """
    This script will create visualization of bip! transaction by stop for each day.
    """
    f = Figlet()
    logger.info(f.renderText('Welcome DTPM'))

    # Arguments and description
    parser = argparse.ArgumentParser(description='create visualization of bip! transaction by stop for each day.')

    parser.add_argument('start_date', help='Lower bound time. For instance 2020-01-01')
    parser.add_argument('end_date', help='Upper bound time. For instance 2020-12-31')
    parser.add_argument('output_filename', help='filename of html file created by the process')

    args = parser.parse_args(argv[1:])

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    output_filename = args.output_filename

    aws_session = AWSSession()
    mapbox_key = config('MAPBOX_KEY')

    # check available days
    dates_in_range = check_available_days(aws_session, start_date, end_date)
    if not dates_in_range:
        logger.error('There is not data between {0} and {1}'.format(start_date, end_date))
        exit(1)
    logger.info('dates found in period: {0}'.format(len(dates_in_range)))

    # get available files
    available_files = get_available_files(dates_in_range, aws_session, DATA_PATH)

    # create output dict
    output, metro_stations, metrotren_stations = get_output_dict(available_files)

    # add location to stop data
    output = add_location_to_stop_data(INPUTS_PATH, output, dates_in_range)

    # add location to metro data
    output = add_location_to_metro_station_data(INPUTS_PATH, output, metro_stations, dates_in_range)

    # add location to metrotren data
    output = add_location_to_metrotren_station_data(INPUTS_PATH, output, dates_in_range)

    # save csv data
    csv_data = create_csv_data(OUTPUTS_PATH, output_filename, output)

    # write mapbox_id to kepler file
    write_info_to_kepler_file(TEMPLATE_PATH, OUTPUTS_PATH, output_filename, mapbox_key, csv_data)

    logger.info('{0} successfully created!'.format(output_filename))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
