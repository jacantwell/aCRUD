import os
from typing import Optional, Tuple, Any

from ..base import StorageBase, StorageConfig
from ..convert import convert, get_type
from ...exception import lookup_handler
from .. import utils


class LocalStorageConfig(StorageConfig):
    root: str


class LocalStorage(StorageBase):
    """
    A CRUD interface for local storage.
    """

    def __init__(self, config: LocalStorageConfig) -> None:
        self.root_dir = config.root

    def ping(self) -> dict:
        return {"response": "pong"}

    def create_file(
        self, key: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Save a file to local storage.
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

        key = os.path.join(self.root_dir, key)

        folder = "/".join(key.split("/")[:-1])

        if not os.path.exists(folder):
            os.makedirs(folder)

        # Save the data
        data = convert(data, bytes)
        with open(key, "wb") as f:
            f.write(data)

        # Save the metadata
        if meta_data is not None:
            meta_data_key = utils.get_meta_data_key(key)
            meta_data = convert(meta_data, bytes)
            with open(meta_data_key, "wb") as f:
                f.write(meta_data)

    def read_file(self, key: str) -> Tuple[Any, Optional[dict]]:
        """
        Read a file from local storage.
        The data will be converted to the appropriate type.

        Args:
            key (str): The path to the file.

        Returns:
            Tuple[Any, Optional[dict]]: The data and, if available, the metadata.
        """

        key = os.path.join(self.root_dir, key)

        try:
            with open(key, "rb") as f:
                obj = f.read()
        except FileNotFoundError:
            lookup_handler(self, key)

        data = convert(obj, get_type(key))  # Converts file data

        # If a metadata file exists, read it
        meta_data_key = utils.get_meta_data_key(key)
        if os.path.exists(meta_data_key):
            with open(meta_data_key, "rb") as f:
                obj = f.read()
            meta_data = convert(obj, dict)
        else:
            meta_data = None

        return data, meta_data

    def update_file(
        self, key: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        TODO: Implement this method.
        Replace the data in a file in S3.

        Args:
            key (str): The path to the file.
            data (Any): The data to save.

        Returns:
            None

        """

        # In Local we simply overwrite the file
        self.create_file(key, data, meta_data)

    def delete_file(self, key: str) -> None:

        full_key = os.path.join(self.root_dir, key)

        # Delete the data
        try:
            os.remove(full_key)
        except LookupError:
            lookup_handler(self, key)

        # Delete the metadata
        meta_data_key = utils.get_meta_data_key(full_key)
        if os.path.exists(meta_data_key):
            os.remove(meta_data_key)

    def list_files_in_directory(self, key: str) -> list:
        full_path = os.path.join(self.root_dir, key)
        if not os.path.exists(full_path):
            return []

        files = os.listdir(full_path)
        # Remove file extensions and metadata files
        files = [
            os.path.splitext(f)[0]
            for f in files
            if not f.startswith(".") and not f.endswith(".meta.json")
        ]
        return list(set(files))

    def list_subdirectories_in_directory(self, key) -> list:

        path = os.path.join(self.root_dir, key)
        files = os.listdir(path)

        # Filter out entries that start with a dot
        files = [file for file in files if not file.startswith(".")]

        return files
