from .s3 import S3GetFile, S3PostFile, S3StorageConfig
from .local import LocalGetFile, LocalPostFile, LocalStorageConfig
from .base import StorageConfig, PostFile, GetFile


def storage_config_factory(
    storage_type: str = None, params: dict = {}
) -> StorageConfig:
    if storage_type == "S3Storage":
        config = S3StorageConfig()
        config.storage_type = storage_type
        config.bucket = params["bucket"]
        return config
    elif storage_type == "LocalStorage":
        config = LocalStorageConfig(
            storage_type=storage_type, root_dir=params["root_dir"]
        )
        return config
    else:
        raise ValueError(f"Storage type not supported.")
