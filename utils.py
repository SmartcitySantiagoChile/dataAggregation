import gzip
import io
import os
import zipfile
import glob


def is_gzipfile(file_path):
    with gzip.open(file_path) as file_obj:
        try:
            file_obj.read(1)
            return True
        except IOError:
            return False


def get_file_object(datafile):
    """
    :return: file object
    """
    if zipfile.is_zipfile(datafile):
        zip_file_obj = zipfile.ZipFile(datafile)
        # it assumes that zip file has only one file
        file_name = zip_file_obj.namelist()[0]
        file_obj_0 = zip_file_obj.open(file_name, 'r')
        file_obj = io.TextIOWrapper(file_obj_0, encoding='latin-1')
    elif is_gzipfile(datafile):
        file_obj = gzip.open(datafile, str('rt'), encoding='latin-1')
    else:
        file_obj = io.open(datafile, str('r'), encoding='latin-1')

    return file_obj


def get_files(file_type, path):
    types = ('*{}'.format(file_type), '*{0}.gz'.format(file_type), '*{0}.zip'.format(file_type))
    files = []
    for file in types:
        files.extend(glob.glob(os.path.join(path, file)))
    return files

