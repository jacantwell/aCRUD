from pydantic import BaseModel


class StorageConfig(BaseModel):
    storage_type: str
