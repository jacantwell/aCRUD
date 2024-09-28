from typing import NoReturn
import os


from ..base import Storage
from ..convert import convert, get_type
from ...schema import LocalGetFile, LocalPostFile, LocalStorageConfig


class LocalStorage(Storage):
    """
    A CRUD interface for local storage.
    """

    def __init__(self, config: LocalStorageConfig) -> None:
        self.root_dir = config.root_dir

    def ping(self) -> dict:
        return {"response": "pong"}

    def create_file(self, file: LocalPostFile) -> LocalPostFile:

        file_path = os.path.join(self.root_dir, file.file_path)

        folder = "/".join(file_path.split("/")[:-1])

        if not os.path.exists(folder):
            os.makedirs(folder)

        # Save the data
        data = convert(file.data, bytes)
        with open(file_path, "wb") as f:
            f.write(data)

        # Save the metadata
        if file.meta_data is not None:
            file_name = file_path.split("/")[-1]
            meta_data_file_path = file_path.replace(file_name, "") + "meta.json"
            meta_data = convert(file.meta_data, bytes)
            with open(meta_data_file_path, "wb") as f:
                f.write(meta_data)

        return file

    def read_file(self, file: LocalGetFile) -> LocalPostFile:

        file_path = os.path.join(self.root_dir, file.file_path)

        # Get the data
        with open(file_path, "rb") as f:
            obj = f.read()
            print(obj)

        data = convert(obj, get_type(file.file_path))  # Converts file data

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

        # Create the file object
        file = LocalPostFile(file_path=file.file_path, data=data, meta_data=meta_data)

        return file

    def update_file(self, file: LocalPostFile) -> LocalPostFile:

        # In Local we simply overwrite the file
        self.create_file(file)

        return file

    def delete_file(self, file_path: str) -> NoReturn:

        full_file_path = os.path.join(self.root_dir, file_path)

        # Delete the data
        if os.path.exists(full_file_path):
            os.remove(full_file_path)
        else:
            raise FileNotFoundError(f"File not found: {full_file_path}")

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

        path = os.path.join(self.root_dir, path)
        files = os.listdir(path)

        # Filter out entries that start with a dot
        files = [file for file in files if not file.startswith(".")]

        return files
