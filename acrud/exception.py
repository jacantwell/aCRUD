from .storage.base import StorageBase


def lookup_handler(storage: StorageBase, file_path: str):

    # Split the file path by '/'
    split_file_path = file_path.split("/")

    for index, path in enumerate(split_file_path):

        if path == "":
            pass

        short_file_path = "/".join(split_file_path[: index + 1])

        try:
            folders = storage.list_subdirectories_in_directory(short_file_path)
        except FileNotFoundError:
            raise LookupError(f"File not found. No such directory as {short_file_path}")

    raise LookupError(f"File not found.")
