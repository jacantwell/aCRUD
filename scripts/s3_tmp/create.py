import argparse

from acrud import create_storage
from acrud.storage.s3_tmp import S3TmpStorageConfig
from acrud.storage.local import LocalStorageConfig

parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_data_local_file_path",
    type=str,
    default="resources/test_files/pdf_file.pdf",
)
parser.add_argument(
    "--output_file_path", type=str, default="tmp/test_files/pdf_file.pdf"
)
parser.add_argument("--meta_data", type=dict, required=False, default=None)
parser.add_argument("--bucket", type=str, default="tl-workflows")

args = parser.parse_args()
input_data_file_path = args.input_data_local_file_path
output_file_path = args.output_file_path
meta_data = args.meta_data
bucket = args.bucket

local_storage_config = LocalStorageConfig(root="./")
local_storage = create_storage(local_storage_config)
s3_storage_config = S3TmpStorageConfig(bucket=bucket)
s3_storage = create_storage(s3_storage_config)


data, meta_data = local_storage.read_file(input_data_file_path)

data_url, meta_url = s3_storage.create_file(output_file_path, data, meta_data)

print(f"Data URL: {data_url}")
print(f"Meta URL: {meta_url}")
