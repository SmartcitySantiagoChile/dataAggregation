# -*- coding: utf8 -*-
import argparse
import csv
import logging
import os
import sys
from collections import defaultdict

from decouple import config
from pyfiglet import Figlet

from aws import AWSSession
from utils import get_file_object, get_files

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
INPUTS_PATH = os.path.join(DIR_PATH, 'inputs')
OUTPUT_PATH = os.path.join(DIR_PATH, 'output')
BUCKET_NAME = config('MISCELLANEOUS_BUCKET_NAME')


def process_trip_data(file_path):
    zone_dict = get_zone_dict(os.path.join(INPUTS_PATH, 'zone_dictionary.csv'))
    trip_data = defaultdict(lambda: defaultdict(float))
    with get_file_object(file_path) as f:
        try:
            next(f)  # skip header
        except StopIteration:
            logger.error("Error: file ", f.name, "is empty.")

        delimiter = str('|')
        reader = csv.reader(f, delimiter=delimiter)

        # skip first row
        next(reader)

        for row in reader:
            trip_value = float(row[1])
            start_commune = zone_dict[row[24]]
            end_commune = zone_dict[row[25]]
            trip_data[start_commune][end_commune] += trip_value
    return dict(trip_data)


def get_zone_dict(path):
    with open(path, 'r', newline='\n', encoding='UTF-8') as inputfile:
        csv_dict = csv.reader(inputfile)
        next(csv_dict)
        zone_dict = {}
        for line in csv_dict:
            code = line[3]
            commune = line[6]
            zone_dict[code] = commune
        return zone_dict


def save_csv_file(data, output, output_filename):
    with open(os.path.join(output, output_filename + '.csv'), 'w', newline='\n', encoding='UTF-8') as outfile:
        w = csv.writer(outfile)
        w.writerow(['Fecha', 'Comuna_origen', 'Comuna_destino', 'N°_viajes_expandidos'])
        for d in data:
            date = "".join(d.split("/")[-1]).split(".")[0]
            data_dict = process_trip_data(d)
            for start_commune in data_dict:
                for end_commune in data_dict[start_commune].keys():
                    w.writerow([date, start_commune, end_commune, data_dict[start_commune][end_commune]])


def send_data_to_s3(path):
    aws_session = AWSSession()
    if not aws_session.check_bucket_exists(BUCKET_NAME):
        print('Bucket \'{0}\' does not exist'.format(BUCKET_NAME))
        exit(1)
    return True


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
    args = parser.parse_args(argv[1:])

    input_path = args.path
    send_to_s3 = args.send_to_s3
    output_path = args.output if args.output else OUTPUT_PATH

    # get data files
    files_path = get_files('trip', input_path)

    # process data and save output
    save_csv_file(files_path, output_path, 'output')

    # send to s3
    # if send_to_s3:
    # send_data_to_s3(os.path.join(output_path, 'output.csv'))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
