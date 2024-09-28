from typing import NoReturn
import os

import boto3
from botocore.exceptions import ClientError

from ..base import Storage
from ..convert import convert, get_type
from ...schema import S3GetFile, S3PostFile, S3StorageConfig


class S3Storage(Storage):
    """
    A CRUD interface for S3.
    """

    def __init__(self, config: S3StorageConfig) -> None:
        self.client = boto3.client("s3")
        self.bucket = config.bucket

    def ping(self) -> dict:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return {"response": "pong"}
        except ClientError as e:
            raise e

    def create_file(self, file: S3PostFile) -> S3PostFile:

        # Save the data
        data = convert(file.data, bytes)
        self.client.put_object(Body=data, Bucket=self.bucket, Key=file.file_path)

        # Save the metadata
        if file.meta_data is not None:
            file_name = file.file_path.split("/")[-1]
            meta_data_file_path = file.file_path.replace(file_name, "") + "meta.json"
            meta_data = convert(file.meta_data, bytes)
            self.client.put_object(
                Body=meta_data, Bucket=self.bucket, Key=meta_data_file_path
            )

        return file

    def read_file(self, file: S3GetFile) -> S3PostFile:

        # Get the data
        obj = self.client.get_object(Bucket=self.bucket, Key=file.file_path)
        obj = obj["Body"].read()
        data = convert(obj, get_type(file.file_path))  # Converts file data

        # Get the metadata
        meta_data_file_path = (
            file.file_path.replace(file.file_path.split("/")[-1], "") + "meta.json"
        )
        obj = self.client.get_object(Bucket=self.bucket, Key=meta_data_file_path)
        obj = obj["Body"]

        if obj is not None:
            meta_data = convert(obj.read(), dict)
        else:
            meta_data = None

        # Create the file object
        file = S3PostFile(file_path=file.file_path, data=data, meta_data=meta_data)
        return file

    def update_file(self, file: S3PostFile) -> S3PostFile:

        # In s3 we simply overwrite the file
        self.create_file(file)

        return file

    def delete_file(self, file_path: str) -> NoReturn:

        # In s3 we simply delete the file
        self.client.delete_object(Bucket=self.bucket, Key=file_path)

    def list_files_in_directory(self, file_path: str) -> list:
        files = self.client.list_objects_v2(Bucket=self.bucket, Prefix=file_path)
        return files

    def list_subdirectories_in_directory(self, file_path) -> list:
        def _extract_next_directory(file_path: str, path: str) -> str:
            directories, _ = os.path.split(file_path)
            directory_list = directories.split("/")
            directory_index = directory_list.index(path)
            # Check if the directory is the last one in the list
            if directory_index == len(directory_list) - 1:
                return None
            else:
                next_directory = directory_list[directory_index + 1]
                return next_directory

        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=file_path)

        last = file_path.split("/")[-1]
        dirs = []

        files = response.get("Contents")
        if files is not None:
            for file in files:
                next_dir = _extract_next_directory(file["Key"], last)
                if next_dir is not None:
                    dirs.append(next_dir)
            dirs = list(set(dirs))  # Remove duplicates

        return dirs
