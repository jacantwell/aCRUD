<p align="center">
  <img src="./resources/logo.png" alt="Logo" width="100"> 
</p>


# aCRUD

## Motivation

The goal of this project is to create a platform agnostic CRUD storage system. This system will allow users to upload files to different platforms (Google Drive, Sharepoint, DigiCloud etc.) without having to worry about the specifics of the platform. This will allow for a more modular system where the storage system can be easily swapped out for another.

## Implementation

#### Storage Interface

Found in the `acrud/schema` directory. This is the interface that all storage systems must implement. This defines the `Files` that can be accepted by the storage system. The `File` class is a base class that all files must inherit from. This class contains the `file_path`, `data` and `meta_data` that will be written to the storage system. The `data` can be any of the supported types (these can currently be found in `settings.py`).

#### Storage System

Found in the `acrud/storage` directory. This is the actual implementation of the storage system. This is where the `File` is written to the storage system. The `Storage` class is the base class that all storage systems must inherit from. This class contains the `create_file` method that must be implemented by the storage system. This method takes in a `File` object and writes the `data` to the storage system. `data` is always converted to a binary file object before being written to the storage system. This is done via the `convert` function which is multi dispatched for different supported type conversions.

Storage systems can be instantiated by passing a `StorageConfig` to the `storage_factory` function. This configuration information is used to create a connection to the selected storage system. For example, the `S3Storage` class requires a `S3StorageConfig` object to be passed to the `storage_factory` function. This object contains the `bucket` which is required to create a connection to the S3 storage system.


#### TODO

- [ ] Implement more graceful error handling.
- [ ] Add support for Microsoft Sharepoint.
- [ ] Add support for Google Drive.
- [ ] Add unit tests.
- [ ] Add logging.

##### Note

Original version of this package can be found on the branch `v0.1.0`.