<p align="center">
  <img src="./resources/logo.png" alt="Logo" width="100"> 
</p>


# aCRUD

This python package provides a CRUD interfaces for a number of storage providers. Currently supported providers:

- [x] Local
- [x] AWS S3
- [x] Google Drive

aCRUD handles the loading and saving of multiple file types, including:

- [x] JSON
- [x] CSV
- [x] PDF
- [x] PKL
- [x] TXT

## Installation

```bash
pip install acrud --extras s3, gdrive, ...
```

```bash
poetry add acrud --extras s3, gdrive, ...
```

## Usage

```python
from acrud import create_storage, get_storage_from_string, S3StorageConfig

# Create a storage config object
# Directly:
config = S3StorageConfig(
    bucket="my-bucket",
)

# Or from a string and a dictionary
config = get_storage_from_string("s3", {"bucket": "my-bucket"})

# Create a storage object
storage = create_storage("s3", config)

# Create a file
storage.create_file("my-file.txt", "Hello, World!")

# Read a file
content = storage.read_file("my-file.txt")

# Update a file
storage.update_file("my-file.txt", "Hello, World! Updated")

# Delete a file
storage.delete_file("my-file.txt")
```

##### Note

Original version of this package can be found on the branch `v0.1.0`.