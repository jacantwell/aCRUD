from abc import ABC
from typing import Any
from pydantic import BaseModel


class StorageConfig(BaseModel):
    storage_type: str


class PostFile(BaseModel):
    file_path: str
    data: Any
    meta_data: dict


class GetFile(BaseModel):
    file_path: str
