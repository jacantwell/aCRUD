from pathlib import Path
from typing import Any, Dict, Optional
import configparser
from importlib import import_module

from acrud.storage.base import StorageBase


class StorageConfig:
    def __init__(self, config_dict: Dict[str, Any]):
        self.storage_type = config_dict.get("STORAGE_TYPE")
        # Add any other common config parameters here
        self.__dict__.update(config_dict)


class StorageFactory:
    @staticmethod
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


def find_config_file() -> Optional[Path]:
    """Search for storage.config file in current and parent directories."""
    current_dir = Path.cwd()

    while current_dir != current_dir.parent:
        config_file = current_dir / "storage.config"
        if config_file.exists():
            return config_file
        current_dir = current_dir.parent

    raise FileNotFoundError(
        "No storage.config file found in current or parent directories"
    )


def load_config() -> Dict[str, Any]:
    """Load configuration from storage.config file."""
    config_file = find_config_file()

    config = configparser.ConfigParser()
    config.read(config_file)

    if "DEFAULT" not in config:
        raise ValueError("Invalid config file format: missing DEFAULT section")

    return dict(config["DEFAULT"])


# Create the storage instance on module import
try:
    config_dict = load_config()
    config = StorageConfig(config_dict)
    storage = StorageFactory.create_storage(config)
except Exception as e:
    raise ImportError(f"Failed to initialize storage: {str(e)}")
