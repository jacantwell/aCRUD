# Set working directory to the directory of this file
import os
from pprint import pprint

os.chdir(os.path.dirname(os.path.abspath(__file__)))

import argparse

from acrud import storage

parser = argparse.ArgumentParser()
parser.add_argument("--file_path", type=str, required=True)

args = parser.parse_args()
file_path = args.file_path

data, meta_data = storage.read_file(file_path)

print("Data:")
pprint(data)

if meta_data:
    print("Meta data:")
    pprint(meta_data)
