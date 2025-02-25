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
        self, key: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Save a file.
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
        pass

    def read_file(self, key: str) -> Tuple[Any, Optional[dict]]:
        """
        Read a file.
        The data will be converted to the appropriate type.

        Args:
            key (str): The path to the file.

        Returns:
            Tuple[Any, Optional[dict]]: The data and, if available, the metadata.
        """
        pass

    def update_file(
        self, key: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Update a file and its metadata.

        Args:
            key (str): The path to the file.
            data (Any): The data to save.

        Returns:
            None

        """
        pass

    def delete_file(self, key: str) -> None:
        """
        Delete a file.

        Args:
            key (str): The path to the file.

        Returns:
            None
        """

    @abstractmethod
    def list_files_in_directory(key: str) -> list:
        pass

    @abstractmethod
    def list_subdirectories_in_directory(key: str) -> list:
        pass
