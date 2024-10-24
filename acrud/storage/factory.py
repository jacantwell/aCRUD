from .base import StorageBase
from .s3 import S3Storage
from .local import LocalStorage
from ..schema import StorageConfig

# Search for a strorage.config file
# If it exists, load the config


def storage_factory(
    config: StorageConfig = None,
) -> StorageBase:
    """
    Factory function that converts a `StorageConfig` into an initialized storage class
    """
    match config.storage_type:
        case "S3Storage":
            return S3Storage(config)
        case "LocalStorage":
            return LocalStorage(config)
