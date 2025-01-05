from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple

from pydantic import BaseModel


class StorageConfig(BaseModel):
    pass


class StorageBase(ABC):

    @abstractmethod
    def ping() -> dict:
        pass

    def create_file(
        self, file_path: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Save a file.
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
        pass

    def read_file(self, file_path: str) -> Tuple[Any, Optional[dict]]:
        """
        Read a file.
        The data will be converted to the appropriate type.

        Args:
            file_path (str): The path to the file.

        Returns:
            Tuple[Any, Optional[dict]]: The data and, if available, the metadata.
        """
        pass

    def update_file(
        self, file_path: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Update a file and its metadata.

        Args:
            file_path (str): The path to the file.
            data (Any): The data to save.

        Returns:
            None

        """
        pass

    def delete_file(self, file_path: str) -> None:
        """
        Delete a file.

        Args:
            file_path (str): The path to the file.

        Returns:
            None
        """

    @abstractmethod
    def list_files_in_directory(file_path: str) -> list:
        pass

    @abstractmethod
    def list_subdirectories_in_directory(file_path: str) -> list:
        pass
