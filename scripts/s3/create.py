import argparse

from acrud import S3StorageConfig, LocalStorageConfig, create_storage

parser = argparse.ArgumentParser()
parser.add_argument("--input_data_local_file_path", type=str, required=True)
parser.add_argument("--output_file_path", type=str, required=True)
parser.add_argument("--meta_data", type=dict, required=False, default=None)
parser.add_argument("--bucket", type=str, required=True)

args = parser.parse_args()
input_data_file_path = args.input_data_local_file_path
output_file_path = args.output_file_path
meta_data = args.meta_data
bucket = args.bucket

local_storage_config = LocalStorageConfig({"root": "./"})
local_storage = create_storage(local_storage_config)
s3_storage_config = S3StorageConfig({"bucket": bucket})
s3_storage = create_storage(s3_storage_config)


data, meta_data = local_storage.read_file(input_data_file_path)

s3_storage.create_file(output_file_path, data, meta_data)
