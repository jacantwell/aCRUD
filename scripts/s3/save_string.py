import sys

from storage import storage_factory

if len(sys.argv) != 4:
    print(
        f"Usage: python {sys.argv[0]} <bucket> <s3_file_path> <local_file_path>",
    )
    exit()

bucket = sys.argv[1]
s3_file_path = sys.argv[2]
local_file_path = sys.argv[3]

# Open the file and read the content
with open(local_file_path, "r") as file:
    content = file.read()

# Ensure that the content is a string
if not isinstance(content, str):
    raise ValueError("Content must be a string")

config = {"storage_type": "S3Storage", "bucket": bucket}
storage = storage_factory(config)
storage.save_string(s3_file_path, content)

print("File saved successfully!")
