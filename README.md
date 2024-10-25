<p align="center">
  <img src="./resources/logo.png" alt="Logo" width="100"> 
</p>


# aCRUD

## Motivation

The goal of this project is to create a platform agnostic CRUD storage system. This system will allow users to upload files to different platforms (Google Drive, Sharepoint, DigiCloud etc.) without having to worry about the specifics of the platform. This will allow for a more modular system where the storage system can be easily swapped out for another.

## Implementation

To use aCRUD you must have a `storage.config` file in your project. This file should contain the configuration information for the storage system you wish to use. Currently this file should be in one of the following formats:

```config
[DEFAULT]
STORAGE_TYPE = local
ROOT = .
```
  
```config
[DEFAULT]
STORAGE_TYPE = s3
BUCKET = my-bucket
```

This file will configure the storage system on import. Now you can simply use the `storage.create_file` and `storage.get_file` functions to interact with the storage system. Changing the platform you are using is as simple as changing the `storage.config` file.

#### TODO

- [ ] Implement more graceful error handling.
- [ ] Documentation.
- [ ] Add support for Microsoft Sharepoint.
- [ ] Add support for Google Drive.
- [ ] Add unit tests.
- [ ] Add logging.

##### Note

Original version of this package can be found on the branch `v0.1.0`.