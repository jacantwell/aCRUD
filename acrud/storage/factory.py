from .base import Storage
from .s3 import S3Storage
from .local import LocalStorage
from ..schema import StorageConfig


def storage_factory(
    config: StorageConfig = None,
) -> Storage:
    if config.storage_type == "S3Storage":
        return S3Storage(config)
    elif config.storage_type == "LocalStorage":
        return LocalStorage(config)
    else:
        raise ValueError(f"Storage type not supported.")
