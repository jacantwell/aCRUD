import argparse
from pprint import pprint

from acrud import S3StorageConfig, create_storage

parser = argparse.ArgumentParser()
parser.add_argument("--file_path", type=str, required=True)
parser.add_argument("--bucket", type=str, required=True)


args = parser.parse_args()
file_path = args.file_path
bucket = args.bucket

s3_storage_config = S3StorageConfig({"bucket": bucket})
s3_storage = create_storage(s3_storage_config)

data, meta_data = s3_storage.read_file(file_path)

print("Data:")
pprint(data)

if meta_data:
    print("Meta data:")
    pprint(meta_data)
