import os
from typing import Optional, Tuple, Any
import dotenv
import urllib.parse
from bson import ObjectId

import pymongo
from pydantic import BaseModel

from ..base import StorageBase
from ..convert import convert, get_type
from .. import utils


# This is stupid and maybe pointless...


class MongoDBStorage(StorageBase):
    """
    A CRUD interface for MongoDB.
    """

    def __init__(self, config: BaseModel) -> None:

        dotenv.load_dotenv()

        user = config.user
        password = config.password
        url = config.url

        user = urllib.parse.quote(user)
        password = urllib.parse.quote(password)

        self.uri = f"mongodb+srv://{user}:{password}@{url}/?retryWrites=true&w=majority"
        self.client = pymongo.MongoClient(self.uri)

        database = self.client[config.database]
        self.collection = database[config.collection]

    def ping(self) -> dict:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return {"response": "pong"}
        except Exception as e:
            raise e

    def create_file(
        self, file_path: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Save a file to S3.
        The data must be of a supported type.
        The meta data, if provided, must be a dictionary.
        The meta data must include the file type, e.g `csv`, `json`, etc.
        The meta data will be save as a field in the document.

        Args:
            file_path (str): The path to the file.
            data (Any): The data to save.
            meta_data (Optional[dict], optional): The meta data to save. Defaults

        Returns:
            None
        """

        # Save the data
        data = convert(data, bytes)
        self.collection.insert_one(
            {"_id": file_path, "data": data, "meta_data": meta_data}
        )

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
        document = self.collection.find_one({"_id": ObjectId(file_path)})
        data = document["data"]
        meta_data = document["meta_data"]

        try:
            file_type = meta_data.pop("file_type")
        except KeyError:
            raise KeyError("The meta data must include the file type.")

        data = convert(data, get_type(file_type))  # Converts file data

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

        # In mongodb we simply overwrite the file
        self.delete_file(file_path)
        self.create_file(file_path, data, meta_data)

    def delete_file(self, file_path: str) -> None:

        # Delete the data
        self.collection.delete_one({"_id": file_path})

        # Delete the metadata
        meta_data_file_path = utils.get_meta_data_file_path(file_path)
        self.client.delete_object(Bucket=self.bucket, Key=meta_data_file_path)

    def list_files_in_directory(self, file_path: str):
        raise NotImplementedError

    def list_subdirectories_in_directory(self, file_path):
        raise NotImplementedError
