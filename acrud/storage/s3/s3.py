import os
from typing import Optional, Tuple, Any

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel

from ..base import StorageBase, StorageConfig
from ..convert import convert, get_type
from .. import utils


class S3StorageConfig(StorageConfig):
    bucket: str


class S3Storage(StorageBase):
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

    def create_file(
        self, file_path: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Save a file to S3.
        The data must be of a supported type.
        The meta data, if provided, must be a dictionary.
        If the file_path is `example/data.csv`, the meta data will be saved to `example/meta.json`.

        Args:
            file_path (str): The path to the file.
            data (Any): The data to save.
            meta_data (Optional[dict], optional): The meta data to save. Defaults

        Returns:
            None
        """

        # Save the data
        try:
            data = convert(data, bytes)
            self.client.put_object(Body=data, Bucket=self.bucket, Key=file_path)
        except Exception as e:
            raise e

        # Save the metadata
        if meta_data is not None:
            try:
                meta_data_file_path = utils.get_meta_data_file_path(file_path)
                meta_data = convert(meta_data, bytes)
                self.client.put_object(
                    Body=meta_data, Bucket=self.bucket, Key=meta_data_file_path
                )
            except Exception as e:
                raise e

    def read_file(self, file_path: str) -> Tuple[Any, Optional[dict]]:
        """
        Read a file from S3.
        The data will be converted to the appropriate type.
        The meta data, if provided, will be converted to a dictionary.

        Args:
            file_path (str): The path to the file.

        Returns:
            Tuple[Any, Optional[dict]]: The data and, if available, the meta data.
        """

        # Get the data
        obj = self.client.get_object(Bucket=self.bucket, Key=file_path)
        obj = obj["Body"].read()
        data = convert(obj, get_type(file_path))  # Converts file data

        # Get the metadata
        try:
            meta_data_file_path = utils.get_meta_data_file_path(file_path)
            obj = self.client.get_object(Bucket=self.bucket, Key=meta_data_file_path)
            meta_data = obj["Body"]

            if meta_data is not None:
                meta_data = convert(meta_data.read(), dict)

        except ClientError:
            print("No metadata found.")
            meta_data = None

        # Create the file object
        return data, meta_data

    def update_file(self, file_path: str, data: Any, meta_data: Optional[dict]) -> None:
        """
        TODO: Implement this method.
        Replace the data in a file in S3.

        Args:
            file_path (str): The path to the file.
            data (Any): The data to save.

        Returns:
            None

        """

        # In s3 we simply overwrite the file
        self.create_file(file_path, data, meta_data)

    def delete_file(self, file_path: str) -> None:

        # Delete the data
        self.client.delete_object(Bucket=self.bucket, Key=file_path)

        # Delete the metadata
        meta_data_file_path = utils.get_meta_data_file_path(file_path)
        self.client.delete_object(Bucket=self.bucket, Key=meta_data_file_path)

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
