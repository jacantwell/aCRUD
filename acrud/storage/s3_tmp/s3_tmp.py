from typing import Optional, Tuple, Any
import requests

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from ..base import StorageBase, StorageConfig
from ..convert import convert, get_type
from .. import utils


class S3TmpStorageConfig(StorageConfig):
    bucket: str


class S3TmpStorage(StorageBase):
    """
    A CRUD interface for S3Tmp.
    """

    def __init__(self, config: S3TmpStorageConfig) -> None:
        self.client: S3Client = boto3.client("s3")
        self.bucket = config.bucket

    def ping(self) -> dict:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return {"response": "pong"}
        except ClientError as e:
            raise e

    def create_file(
        self, key: str, data: Any, meta_data: Optional[dict] = None
    ) -> Tuple[str, Optional[str]]:
        """
        Save a file to S3Tmp.
        The data must be of a supported type.
        The meta data, if provided, must be a dictionary.
        If the key is `example/data.csv`, the meta data will be saved to `example/meta.json`.

        Args:
            key (str): The path to the file.
            data (Any): The data to save.
            meta_data (Optional[dict], optional): The meta data to save. Defaults

        Returns:
            None
        """

        # Save the data
        try:
            data = convert(data, bytes)
            self.client.put_object(Body=data, Bucket=self.bucket, Key=key)
        except Exception as e:
            raise e

        # Save the metadata
        if meta_data is not None:
            try:
                meta_data_key = utils.get_meta_data_key(key)
                meta_data = convert(meta_data, bytes)
                self.client.put_object(
                    Body=meta_data, Bucket=self.bucket, Key=meta_data_key
                )
                meta_data_url = self.client.generate_presigned_url(
                    "get_object", Params={"Bucket": self.bucket, "Key": meta_data_key}
                )
            except Exception as e:
                raise e
        else:
            meta_data_url = None

        # Generate and return a presigned URL
        data_url = self.client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}
        )
        return data_url, meta_data_url

    def read_file(
        self, key: str, meta_data_key: Optional[str] = None
    ) -> Tuple[Any, Optional[dict]]:
        """
        Read a file from S3Tmp.
        The data will be converted to the appropriate type.
        The meta data, if provided, will be converted to a dictionary.

        Args:
            key (str): The path to the file.

        Returns:
            Tuple[Any, Optional[dict]]: The data and, if available, the meta data.
        """

        # Get the data
        try:
            response = requests.get(key)
            data = response.content
            data = convert(data, get_type(key))
        except Exception as e:
            raise e

        # Get the metadata
        if meta_data_key is not None:
            try:
                response = requests.get(meta_data_key)
                meta_data = response.content
                meta_data = convert(meta_data, dict)
            except Exception as e:
                raise e
        else:
            meta_data = None

        return data, meta_data

    def update_file(self, key: str, data: Any, meta_data: Optional[dict]) -> None:
        pass

    def delete_file(self, key: str) -> None:
        pass

    def list_files_in_directory(self, key: str) -> list:
        pass

    def list_subdirectories_in_directory(self, key) -> list:
        pass
