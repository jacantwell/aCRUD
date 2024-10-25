import os
from typing import Optional, Tuple, Any

from ..base import StorageBase
from ..convert import convert, get_type
from ...exception import lookup_handler


class LocalStorage(StorageBase):
    """
    A CRUD interface for local storage.
    """

    def __init__(self, config) -> None:
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
            file_name = file_path.split("/")[-1]
            meta_data_file_path = file_path.replace(file_name, "") + "meta.json"
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

        # Get the metadata
        meta_data_file_path = (
            file_path.replace(file_path.split("/")[-1], "") + "meta.json"
        )

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
        full_file_path = full_file_path.split("/")[-1]
        meta_data_file_path = file_path.replace(full_file_path, "") + "meta.json"
        if os.path.exists(meta_data_file_path):
            os.remove(meta_data_file_path)

    def list_files_in_directory(self, file_path: str) -> list:
        path = os.path.join(self.root_dir, path)

        files = os.listdir(path)

        files = ["".join(file.split("/")[-1].split(".")[:-1]) for file in files]
        files = list(set(files))
        return files

    def list_subdirectories_in_directory(self, file_path) -> list:

        path = os.path.join(self.root_dir, file_path)
        files = os.listdir(path)

        # Filter out entries that start with a dot
        files = [file for file in files if not file.startswith(".")]

        return files
