# -*- coding: utf8 -*-
import argparse
import csv
import logging
import os
import sys
from utils import get_file_object, get_files
from pyfiglet import Figlet
from aws import AWSSession
from decouple import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
INPUTS_PATH = os.path.join(DIR_PATH, 'inputs')
OUTPUT_PATH = os.path.join(DIR_PATH, 'output')
BUCKET_NAME = config('MISCELLANEOUS_BUCKET_NAME')


def process_general_data(file_path):
    with get_file_object(file_path) as f:
        try:
            next(f)  # skip header
        except StopIteration:
            logger.error("Error: file ", f.name, "is empty.")
        delimiter = str('|')
        reader = csv.reader(f, delimiter=delimiter)
        row = next(reader)
        return [row[0], row[12]]


def save_csv_file(data, output, output_filename):
    with open(os.path.join(output, output_filename + '.csv'), 'w', newline='\n', encoding='latin-1') as outfile:
        w = csv.writer(outfile)
        w.writerow(['Fecha', 'Transacciones'])
        for d in data:
            w.writerow(d)


def main(argv):
    """
    This script will create a csv file with number of transactions for each day.
    """
    f = Figlet()
    logger.info(f.renderText('Transacitions for each Day'))

    # Arguments and description
    parser = argparse.ArgumentParser(description='create csv file with number of transactions for each day.')

    parser.add_argument('path', help='Path for .general files.')
    parser.add_argument('--send-to-s3', help='Send file to S3 bucket.', action='store_true')
    parser.add_argument('--output', default=None,
                        help='path where files will be saved, if it is not provided we will use output path')
    args = parser.parse_args(argv[1:])

    input_path = args.path
    send_to_s3 = args.send_to_s3
    output_path = args.output if args.output else OUTPUT_PATH

    # process data
    files_path = get_files('general', input_path)
    files = [process_general_data(file) for file in files_path]

    # save output
    save_csv_file(files, output_path, 'output')

    # send to s3
    if send_to_s3:
        aws = AWSSession()



if __name__ == "__main__":
    sys.exit(main(sys.argv))
