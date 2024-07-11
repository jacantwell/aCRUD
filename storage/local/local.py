from io import BytesIO
from typing import Any
import dill
import os

from ..base import Storage


class LocalStorage(Storage):
    def __init__(self, config: dict) -> None:
        self.root_dir = config["root_dir"]

    def serialize(self) -> dict:
        return {"storage_type": self.__class__.__name__, "root_dir": self.root_dir}

    def ping(self) -> bool:
        return True

    def list_files_in_directory(self, path: str) -> list:
        path = os.path.join(self.root_dir, path)
        try:
            files = os.listdir(path)
        except:
            raise LookupError(f"Unable to list files in your account: {path}")

        files = ["".join(file.split("/")[-1].split(".")[:-1]) for file in files]
        files = list(set(files))

    def list_subdirectories_in_directory(self, path: str) -> list:
        path = os.path.join(self.root_dir, path)
        try:
            files = os.listdir(path)
            # Filter out entries that start with a dot
            files = [file for file in files if not file.startswith(".")]
        except:
            raise LookupError(f"Unable to list directories in your account: {path}")
        return files

    def save_string(self, path: str, content: str) -> None:
        path = os.path.join(self.root_dir, path)

        folder = "/".join(path.split("/")[:-1])
        print("\n\n folder", folder)
        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(path, "w") as file:
            file.write(content)

    def load_string(self, path: str) -> str:
        path = os.path.join(self.root_dir, path)
        with open(path, "r") as file:
            return file.read()

    def save_object(self, path: str, obj: Any) -> None:
        path = os.path.join(self.root_dir, path)

        folder = "/".join(path.split("/")[:-1])
        if not os.path.exists(folder):
            os.makedirs(folder)

        try:
            buffer = BytesIO()
            dill.dump(obj, buffer)
            buffer.seek(0)
        except:
            raise ValueError(f"Unable to serialize object: {obj}")

        with open(path, "wb") as file:
            file.write(buffer.read())

    def load_object(self, path: str) -> Any:
        path = os.path.join(self.root_dir, path)
        with open(path, "rb") as file:
            return dill.load(file)

    def delete_directory(self, path: str) -> None:

        # This needs fixing - currently doesnt delete everything in the directory

        path = os.path.join(self.root_dir, path)
        try:
            os.rmdir(path)
        except:
            raise ValueError(f"Unable to delete directory: {path}")

    def check_file_exists(self, path: str) -> bool:
        path = os.path.join(self.root_dir, path)
        return os.path.isfile(path)
