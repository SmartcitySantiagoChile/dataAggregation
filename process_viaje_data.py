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


def get_commune_for_extra_location(row, start_commune, end_commune):
    errors = set()
    with open(os.path.join(INPUTS_PATH, 'extra_location_communes.json')) as communes_json:
        extra_location_communes = json.load(communes_json)
        if not start_commune:
            start_commune = extra_location_communes.get(row[10].title(), None)
        if not end_commune:
            end_commune = extra_location_communes.get(row[11].title(), None)
    if not start_commune:
        errors.add(row[10].title())
    if not end_commune:
        errors.add(row[11].title())
    return errors, start_commune, end_commune


def process_viaje_data(file_path):
    trip_data = defaultdict(lambda: defaultdict(float))
    errors = set()
    with open(os.path.join(INPUTS_PATH, 'communes.json')) as communes_json:
        try:
            f = get_file_object(file_path)
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.reader(f, delimiter=delimiter)
            next(reader)
        except (IndexError, StopIteration):
            logging.warning("{0} is empty.".format(os.path.basename(file_path)))
            return None, errors

        for row in reader:
            trip_value = float(row[23])
            start_commune = row[12].title().rstrip() if row[12] != '-' else None
            end_commune = row[13].title().rstrip() if row[13] != '-' else None
            if start_commune == 'Nunoa':
                start_commune = 'Ñuñoa'
            if end_commune == 'Nunoa':
                end_commune = 'Ñuñoa'
            new_errors = set()
            if not start_commune or not end_commune:
                new_errors, start_commune, end_commune = get_commune_for_extra_location(row, start_commune,
                                                                                        end_commune)
            if not new_errors:
                trip_data[start_commune][end_commune] += trip_value
            errors.update(new_errors)
        f.close()
    return dict(trip_data), errors


def save_csv_file(data, output, output_filename):
    name = os.path.join(output, output_filename)
    csv_name = '{0}.csv'.format(name)
    gz_name = '{0}.csv.gz'.format(name)
    gz_actual_name = '{0}.gz'.format(name)
    with open(csv_name, 'w+', newline='\n') as outfile:
        w = csv.writer(outfile)
        w.writerow(['Fecha', 'Comuna_origen', 'Comuna_destino', 'N°_viajes_expandidos'])
    errors = set()
    for d in data:
        date = "".join(os.path.basename(d)).split(".")[0]
        logger.info("Processing date {0}...".format(date))
        data_dict, new_errors = process_viaje_data(d)
        errors.update(new_errors)
        if data_dict:
            with open(csv_name, 'a+', newline='\n') as outfile:
                w = csv.writer(outfile)
                for start_commune in data_dict:
                    for end_commune in data_dict[start_commune].keys():
                        w.writerow([date, start_commune, end_commune, data_dict[start_commune][end_commune]])
    for e in errors:
        logger.warning("{0} has no commune.".format(e))

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
    files_path = get_files('viajes', input_path)

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
