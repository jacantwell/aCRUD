from typing import Any, Dict
from importlib import import_module

from acrud.storage.base import StorageBase


class StorageConfig:
    def __init__(self, config_dict: Dict[str, Any]):
        self.storage_type = config_dict.get("storage_type")
        # Add any other common config parameters here
        self.__dict__.update(config_dict)


def create_storage(config: StorageConfig) -> StorageBase:
    storage_type = config.storage_type.lower()
    package = "acrud.storage"
    # Dynamically import the appropriate storage module
    try:
        module = import_module(package + "." + storage_type, package)
        storage_class = getattr(module, f"{storage_type.capitalize()}Storage")
        return storage_class(config)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Unsupported storage type: {config.storage_type}") from e
