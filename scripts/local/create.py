# Set working directory to the directory of this file
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

import argparse

from acrud import storage

parser = argparse.ArgumentParser()
parser.add_argument("--input_data_file_path", type=str, required=True)
parser.add_argument("--output_file_path", type=str, required=True)
parser.add_argument("--meta_data", type=dict, required=False, default=None)

args = parser.parse_args()
input_data_file_path = args.input_data_file_path
output_file_path = args.output_file_path
meta_data = args.meta_data

data, _ = storage.read_file(input_data_file_path)

storage.create_file(output_file_path, data, meta_data)
