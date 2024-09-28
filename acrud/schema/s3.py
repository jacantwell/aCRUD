from .base import GetFile, PostFile, StorageConfig


class S3StorageConfig(StorageConfig):
    bucket: str


class S3PostFile(PostFile):
    pass


class S3GetFile(GetFile):
    pass
