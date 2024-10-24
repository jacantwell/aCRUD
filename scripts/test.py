from acrud import storage

# Create a storage config
params = {"root_dir": "/Users/jaspercantwell/repos/aCRUD/test_data"}

# config = storage_config_factory(storage_type="LocalStorage", params=params)

# # Create a storage object
# storage = storage_factory(config)

# Ping the storage
response = storage.ping()

# Create a file
file = {
    "file_path": "real_f/test_data.txt",
    "data": "Hello, World!",
    "meta_data": {"author": "Jasper Cantwell"},
}

file = storage.create_file(file["file_path"], file["data"], file["meta_data"])

# Read the file

file = {"file_path": "real_f/test_data.txt"}

print("Reading file")
file = storage.read_file(file["file_path"])

# print(file.dict())
