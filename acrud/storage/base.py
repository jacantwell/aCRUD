from abc import ABC, abstractmethod


class StorageBase(ABC):

    @abstractmethod
    def ping() -> dict:
        pass

    @abstractmethod
    def list_files_in_directory() -> list:
        pass

    @abstractmethod
    def list_subdirectories_in_directory() -> list:
        pass

    @abstractmethod
    def create_file() -> None:
        pass

    @abstractmethod
    def read_file() -> str:
        pass

    @abstractmethod
    def update_file() -> None:
        pass

    @abstractmethod
    def delete_file() -> None:
        pass
