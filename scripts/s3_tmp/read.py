import argparse
from pprint import pprint
from PyPDF2 import PdfFileReader

from acrud import create_storage, get_config_from_str

parser = argparse.ArgumentParser()
parser.add_argument("--key", type=str, required=True)
parser.add_argument("--bucket", type=str, default="tl-workflows")


args = parser.parse_args()
key = args.key
bucket = args.bucket

s3_storage_config = get_config_from_str("s3_tmp", {"bucket": bucket})
s3_storage = create_storage(s3_storage_config)

data, meta_data = s3_storage.read_file(key)

print("Data:")
pprint(data)

data: PdfFileReader = data

data = data.metadata
print(data)

if meta_data:
    print("Meta data:")
    pprint(meta_data)
