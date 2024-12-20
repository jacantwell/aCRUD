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
        self, file_path: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Save a file to local storage.
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

        file_path = os.path.join(self.root_dir, file_path)

        folder = "/".join(file_path.split("/")[:-1])

        if not os.path.exists(folder):
            os.makedirs(folder)

        # Save the data
        data = convert(data, bytes)
        with open(file_path, "wb") as f:
            f.write(data)

        # Save the metadata
        if meta_data is not None:
            meta_data_file_path = utils.get_meta_data_file_path(file_path)
            meta_data = convert(meta_data, bytes)
            with open(meta_data_file_path, "wb") as f:
                f.write(meta_data)

    def read_file(self, file_path: str) -> Tuple[Any, Optional[dict]]:
        """
        Read a file from local storage.
        The data will be converted to the appropriate type.

        Args:
            file_path (str): The path to the file.

        Returns:
            Tuple[Any, Optional[dict]]: The data and, if available, the metadata.
        """

        file_path = os.path.join(self.root_dir, file_path)

        try:
            with open(file_path, "rb") as f:
                obj = f.read()
        except FileNotFoundError:
            lookup_handler(self, file_path)

        data = convert(obj, get_type(file_path))  # Converts file data

        # If a metadata file exists, read it
        meta_data_file_path = utils.get_meta_data_file_path(file_path)
        if os.path.exists(meta_data_file_path):
            with open(meta_data_file_path, "rb") as f:
                obj = f.read()
            meta_data = convert(obj, dict)
        else:
            meta_data = None

        return data, meta_data

    def update_file(
        self, file_path: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        TODO: Implement this method.
        Replace the data in a file in S3.

        Args:
            file_path (str): The path to the file.
            data (Any): The data to save.

        Returns:
            None

        """

        # In Local we simply overwrite the file
        self.create_file(file_path, data, meta_data)

    def delete_file(self, file_path: str) -> None:

        full_file_path = os.path.join(self.root_dir, file_path)

        # Delete the data
        try:
            os.remove(full_file_path)
        except LookupError:
            lookup_handler(self, file_path)

        # Delete the metadata
        meta_data_file_path = utils.get_meta_data_file_path(full_file_path)
        if os.path.exists(meta_data_file_path):
            os.remove(meta_data_file_path)

    def list_files_in_directory(self, file_path: str) -> list:
        full_path = os.path.join(self.root_dir, file_path)
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

    def list_subdirectories_in_directory(self, file_path) -> list:

        path = os.path.join(self.root_dir, file_path)
        files = os.listdir(path)

        # Filter out entries that start with a dot
        files = [file for file in files if not file.startswith(".")]

        return files
