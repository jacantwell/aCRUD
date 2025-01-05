import argparse
from pprint import pprint

from acrud import LocalStorageConfig, create_storage

parser = argparse.ArgumentParser()
parser.add_argument("--file_path", type=str, required=True)

args = parser.parse_args()
file_path = args.file_path

local_storage_config = LocalStorageConfig()
local_storage = create_storage(local_storage_config)

data, meta_data = local_storage.read_file(file_path)

print("Data:")
pprint(data)

if meta_data:
    print("Meta data:")
    pprint(meta_data)
