from typing import Any
from io import BytesIO
import os

from typeguard import typechecked
import boto3
import dill

from ..base import Storage


@typechecked
class S3Storage(Storage):
    """
    A storage class that uses s3 as a storage backend.
    To use this class you must have your aws credentials configured.
    """

    def __init__(self, config: dict) -> None:
        self.client = boto3.client("s3")
        self.bucket = config["bucket"]

    def serialize(self) -> dict:
        return {"storage_type": self.__class__.__name__, "bucket": self.bucket}

    def ping(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except:
            return False

    def list_files_in_directory(self, path: str) -> list:
        """
        List files in a directory of an S3 bucket.
        """
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=path)
        except self.client.exceptions.NoSuchBucket:
            raise LookupError(f"Bucket `{self.bucket}` not found")

        files = response.get("Contents")

        # In s3 if a directory is empty it does not exist. So we raise an error.
        if files is None:
            self._handle_path_not_found_exception(path)

        files = ["".join(file["Key"].split("/")[-1].split(".")[:-1]) for file in files]
        files = list(set(files))  # Remove duplicates

        return files

    def list_subdirectories_in_directory(self, path: str) -> list:
        """
        List subdirectories in S3 bucket.
        """

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

        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=path)
        except self.client.exceptions.NoSuchBucket:
            raise LookupError(f"Bucket `{self.bucket}` not found")

        files = response.get("Contents")
        if files is None:
            self._handle_path_not_found_exception(path)

        last = path.split("/")[-1]
        dirs = []
        for file in files:
            next_dir = _extract_next_directory(file["Key"], last)
            if next_dir is not None:
                dirs.append(next_dir)
        dirs = list(set(dirs))  # Remove duplicates

        return dirs

    def save_string(self, path: str, string: str) -> None:
        """
        Save a string to S3.
        """
        try:
            file_object = bytes(string, "utf-8")
            response = self.client.put_object(
                Body=file_object, Bucket=self.bucket, Key=path
            )
        except self.client.exceptions.NoSuchBucket:
            raise Exception(f"Unable to save string. Bucket `{self.bucket}` not found")
        print(response)

    def load_string(self, path: str) -> str:
        """
        Read a file from S3 as a string.
        """
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=path)
            string = obj["Body"].read().decode("utf-8")
            return string
        except self.client.exceptions.NoSuchBucket:
            self._handle_path_not_found_exception(path)

    def save_object(self, path: str, obj: Any) -> None:
        """
        Save a dill-pickled object to S3.
        """

        try:
            buffer = BytesIO()
            dill.dump(obj, buffer)
            buffer.seek(0)
            self.client.put_object(Body=buffer.getvalue(), Bucket=self.bucket, Key=path)
        except:
            raise Exception(f"Unable to save pickled object to path {path}")

    def load_object(self, path: str) -> Any:
        """
        Load a dill-pickled object from S3.
        """

        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=path)
            return obj
        except self.client.exceptions.NoSuchBucket:
            self._handle_path_not_found_exception(path)

        try:
            data = obj["Body"].read()
            data = BytesIO(data)
            obj = dill.load(data)
        except:
            raise Exception(f"Unable to un-pickle object {path}")

    def delete_directory(self, path: str) -> None:
        """
        Delete a directory and all of its contents.
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=path)

        except self.client.exceptions.NoSuchBucket:
            self._handle_path_not_found_exception(path)

        delete_us = dict(Objects=[])
        for item in pages.search("Contents"):
            if item is not None:
                delete_us["Objects"].append(dict(Key=item["Key"]))

            # flush once aws limit reached
            if len(delete_us["Objects"]) >= 1000:
                self.client.delete_objects(Bucket=self.bucket, Delete=delete_us)
                delete_us = dict(Objects=[])

        # flush rest
        if len(delete_us["Objects"]):
            self.client.delete_objects(Bucket=self.bucket, Delete=delete_us)

    def check_file_exists(self, path: str) -> bool:
        """
        Check if a file exists in S3.
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=path)
            return True
        except:
            return False

    def _generate_presigned_get_url(self, path: str) -> str:
        """
        Generate a presigned URL for downloading a file from S3.
        """
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": path},
                ExpiresIn=3600,
            )
        except:
            raise Exception(
                f"Unable to generate a presigned URL for your account: {path}"
            )
        return url

    def _generate_presigned_put_url(self, path: str) -> str:
        """
        Generate a presigned URL for downloading a file from S3.
        """

        try:
            url = self.client.generate_presigned_url(
                "put_object",
                Params={"Bucket": self.bucket, "Key": path},
                ExpiresIn=3600,
            )
        except:
            raise Exception(
                f"Unable to generate a presigned URL for your account: {path}"
            )
        return url

    def generate_presigned_url(self, path: str, method: str) -> str:
        """
        Generate a presigned URL for uploading a file to S3.
        """

        match method:
            case "GET":
                url = self._generate_presigned_get_url(path)
            case "PUT":
                url = self._generate_presigned_put_url(path)
            case _:
                raise ValueError("Method must be either 'GET' or 'PUT'.")

        return url

    def _handle_path_not_found_exception(self, path):

        # In order to return a nice error message we iterate over the path and find which directory is missing
        path_list = path.split("/")
        for i in range(1, len(path_list) + 1):
            new_path = "/".join(path_list[:i])
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=new_path)
            files = response.get("Contents")
            if files is None:
                raise LookupError(
                    f"Unable to find directory `{new_path}` in your account"
                )
        raise LookupError(f"Unable to find directory `{path}` in your account")
