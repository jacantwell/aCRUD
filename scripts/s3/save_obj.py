import sys

import dill

from storage import storage_factory

if len(sys.argv) != 4:
    print(
        f"Usage: python {sys.argv[0]} <bucket> <s3_filepath> <local_pkl_filepath>",
    )
    exit()

bucket = sys.argv[1]
s3_filepath = sys.argv[2]
local_filepath = sys.argv[3]

# Open the .pkl file and load the content
with open(local_filepath, "rb") as file:
    content = dill.loads(file.read())

config = {"storage_type": "S3Storage", "bucket": bucket}
storage = storage_factory(config)
storage.save_object(s3_filepath, content)

print("File saved successfully!")
