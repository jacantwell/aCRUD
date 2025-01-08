from importlib import import_module
from stringcase import pascalcase
from typing import Any, Dict

from acrud.storage.base import StorageBase, StorageConfig


def get_config_from_str(storage_type: str, config: Dict[str, Any]) -> StorageConfig:
    package = "acrud.storage"
    try:
        module = import_module(package + "." + storage_type, package)
        storage_config_class = getattr(
            module, f"{pascalcase(storage_type)}StorageConfig"
        )
        return storage_config_class(**config)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Unsupported storage type: {storage_type}") from e


def create_storage(config: StorageConfig) -> StorageBase:
    package = "acrud.storage"
    storage_type = config.__class__.__name__.replace("StorageConfig", "").lower()
    # Dynamically import the appropriate storage module
    try:
        module = import_module(package + "." + storage_type, package)
        storage_class = getattr(module, f"{pascalcase(storage_type)}Storage")
        return storage_class(config)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Unsupported storage type: {storage_type}") from e
