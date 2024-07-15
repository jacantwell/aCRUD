import sys

from storage import storage_factory

if len(sys.argv) != 3:
    print(
        f"Usage: python {sys.argv[0]} <bucket> <s3_filepath>",
    )
    exit()

bucket = sys.argv[1]
s3_filepath = sys.argv[2]

config = {"storage_type": "S3Storage", "bucket": bucket}
storage = storage_factory(config)

data = storage.load_string(s3_filepath)
print(data)
