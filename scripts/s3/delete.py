import argparse

from acrud import S3StorageConfig, create_storage

parser = argparse.ArgumentParser()
parser.add_argument("--file_path", type=str, required=True)
parser.add_argument("--bucket", type=str, required=True)

args = parser.parse_args()
file_path = args.file_path
bucket = args.bucket

s3_storage_config = S3StorageConfig({"bucket": bucket})
s3_storage = create_storage(s3_storage_config)

s3_storage.delete_file(file_path)

print("File deleted successfully.")
