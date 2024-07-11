from typeguard import typechecked

from .s3 import S3Storage
from .local import LocalStorage
from .base import Storage


@typechecked
def storage_factory(
    config: dict = None,
) -> Storage:
    if config["storage_type"] == "S3Storage":
        return S3Storage(config)
    elif config["storage_type"] == "LocalStorage":
        return LocalStorage(config)
    else:
        raise ValueError(f"Storage type not supported.")
