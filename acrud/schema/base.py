from typing import Any, Optional

from pydantic import BaseModel


class StorageConfig(BaseModel):
    storage_type: str


class PostFile(BaseModel):
    file_path: str
    data: Any
    meta_data: Optional[dict] = None


class GetFile(BaseModel):
    file_path: str
