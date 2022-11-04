import json
import os
import shutil

from pathlib import Path


def create_directory(destination_directory):
    dirpath = Path(destination_directory)
    if dirpath.exists():
        shutil.rmtree(dirpath)

    os.mkdir(dirpath)


def save_data(data, destination_directory, destination_filename):
    file_path = os.path.join(destination_directory, destination_filename)
    with open(file_path, 'w') as data_file:
        json.dump(data, data_file)
