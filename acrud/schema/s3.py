from .base import StorageConfig


class S3StorageConfig(StorageConfig):
    bucket: str
