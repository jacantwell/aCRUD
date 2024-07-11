import sys

from storage import storage_factory

if len(sys.argv) != 3:
    print(
        f"Usage: python {sys.argv[0]} <bucket> <directory>",
    )
    exit()

bucket = sys.argv[1]
directory = sys.argv[2]

config = {"storage_type": "S3Storage", "bucket": bucket}
storage = storage_factory(config)

files = storage.list_files_in_directory(directory)
for file in files:
    print(file)
