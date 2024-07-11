from abc import ABC, abstractmethod
from typing import Any


class Storage(ABC):

    @abstractmethod
    def ping() -> bool:
        pass

    @abstractmethod
    def list_files_in_directory() -> list:
        pass

    @abstractmethod
    def list_subdirectories_in_directory() -> list:
        pass

    @abstractmethod
    def save_string() -> None:
        pass

    @abstractmethod
    def load_string() -> str:
        pass

    @abstractmethod
    def save_object() -> None:
        pass

    @abstractmethod
    def load_object() -> Any:
        pass

    @abstractmethod
    def delete_directory() -> None:
        pass
