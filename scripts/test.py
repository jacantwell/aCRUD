from acrud import storage_factory, storage_config_factory
from acrud.schema import LocalPostFile, LocalGetFile

# Create a storage config
params = {"root_dir": "/Users/jaspercantwell/repos/aCRUD/test_data"}

config = storage_config_factory(storage_type="LocalStorage", params=params)

# Create a storage object
storage = storage_factory(config)

# Ping the storage
response = storage.ping()

print(response)

# Create a file
file = {
    "file_path": "test_data.txt",
    "data": "Hello, World!",
    "meta_data": {"author": "Jasper Cantwell"},
}

file = LocalPostFile(**file)

file = storage.create_file(file)

# Read the file
file = {"file_path": "test_data.txt"}

file = LocalGetFile(**file)

file = storage.read_file(file)

print(file.dict())
