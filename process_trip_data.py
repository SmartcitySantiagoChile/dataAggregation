# -*- coding: utf8 -*-
import argparse
import csv
import gzip
import json
import logging
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime

from decouple import config
from pyfiglet import Figlet

from utils import get_file_object, get_files, send_data_to_s3, valid_date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
INPUTS_PATH = os.path.join(DIR_PATH, 'inputs')
OUTPUT_PATH = os.path.join(DIR_PATH, 'output')
BUCKET_NAME = config('MISCELLANEOUS_BUCKET_NAME')
OUTPUT_NAME = 'viajesEntreComunas'


def get_commune_for_metrotren_station(row, start_commune, end_commune):
    is_metrotren = True
    with open(os.path.join(INPUTS_PATH, 'metrotren_communes.json')) as communes_json:
        metrotren_communes_dict = json.load(communes_json)
        if not start_commune:
            start_commune = metrotren_communes_dict.get(row[20], None)
        if not end_commune:
            end_commune = metrotren_communes_dict.get(row[21], None)
    if not start_commune:
        is_metrotren = False
        logger.warning("{0} has not commune. ".format(row[20]))
    if not end_commune:
        is_metrotren = False
        logger.warning("{0} has not commune. ".format(row[21]))

    return is_metrotren, start_commune, end_commune


def process_trip_data(file_path):
    trip_data = defaultdict(lambda: defaultdict(float))
    with open(os.path.join(INPUTS_PATH, 'communes.json')) as communes_json:
        communes_dict = json.load(communes_json)
        try:
            f = get_file_object(file_path)
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.reader(f, delimiter=delimiter)
            next(reader)
        except (IndexError, StopIteration):
            logging.warning("{0} is empty.".format(os.path.basename(file_path)))
            return None

        for row in reader:
            trip_value = float(row[1])
            start_commune = communes_dict.get(row[22], None)
            end_commune = communes_dict.get(row[23], None)
            valid = True
            if not start_commune or not end_commune:
                valid, start_commune, end_commune = get_commune_for_metrotren_station(row, start_commune,
                                                                                      end_commune)
            if valid:
                trip_data[start_commune][end_commune] += trip_value

        f.close()
    return dict(trip_data)


def save_csv_file(data, output, output_filename):
    name = os.path.join(output, output_filename)
    csv_name = '{0}.csv'.format(name)
    gz_name = '{0}.csv.gz'.format(name)
    gz_actual_name = '{0}.gz'.format(name)
    with open(csv_name, 'w+', newline='\n') as outfile:
        w = csv.writer(outfile)
        w.writerow(['Fecha', 'Comuna_origen', 'Comuna_destino', 'N°_viajes_expandidos'])

    for d in data:
        date = "".join(os.path.basename(d)).split(".")[0]
        logger.info("Processing date {0}...".format(date))
        data_dict = process_trip_data(d)
        if data_dict:
            with open(csv_name, 'a+', newline='\n') as outfile:
                w = csv.writer(outfile)
                for start_commune in data_dict:
                    for end_commune in data_dict[start_commune].keys():
                        w.writerow([date, start_commune, end_commune, data_dict[start_commune][end_commune]])

    with open(csv_name, 'rb') as f_in:
        with gzip.open(gz_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.rename(gz_name, gz_actual_name)
    os.remove(csv_name)


def main(argv):
    """
    This script will create a csv file with number of expanded trips for each commune per day.
    """
    f = Figlet()
    logger.info(f.renderText('Trips for each Commune per Day'))

    # Arguments and description
    parser = argparse.ArgumentParser(
        description='create csv file with number of expanded trips for each commune per day.')

    parser.add_argument('path', help='Path for .trip files.')
    parser.add_argument('--send-to-s3', help='Send file to S3 bucket.', action='store_true')
    parser.add_argument('--output', default=None,
                        help='path where files will be saved, if it is not provided we will use output path')
    parser.add_argument('--lower-bound', help='Lower bound date to process in YYYY-MM-DD format .', default=None,
                        type=valid_date)
    parser.add_argument('--upper-bound', help='Upper bound date to process in YYYY-MM-DD format .', default=None,
                        type=valid_date)
    args = parser.parse_args(argv[1:])

    input_path = args.path
    send_to_s3 = args.send_to_s3
    output_path = args.output if args.output else OUTPUT_PATH
    lower_bound = args.lower_bound
    upper_bound = args.upper_bound

    if len([x for x in (lower_bound, upper_bound) if x is not None]) == 1:
        parser.error('--lower-bound and --upper-bound must be given together')

    if lower_bound and upper_bound and lower_bound > upper_bound:
        parser.error('lower-bound must be lower than upper-bound ')

    # get data files
    files_path = get_files('trip', input_path)

    # filter between dates
    if lower_bound:
        files_path = [file for file in files_path if
                      lower_bound <= datetime.strptime(os.path.basename(file).split(".")[0],
                                                       '%Y-%m-%d') <= upper_bound]
    # process data and save output
    save_csv_file(files_path, output_path, OUTPUT_NAME)

    # send to s3
    if send_to_s3:
        send_data_to_s3(os.path.join(output_path, '{0}.gz'.format(OUTPUT_NAME)), BUCKET_NAME)

    logger.info('{0} successfully created!'.format('{0}.gz'.format(OUTPUT_NAME)))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
