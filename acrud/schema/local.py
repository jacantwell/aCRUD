from .base import GetFile, PostFile, StorageConfig


class LocalStorageConfig(StorageConfig):
    root_dir: str


class LocalPostFile(PostFile):
    pass


class LocalGetFile(GetFile):
    pass
