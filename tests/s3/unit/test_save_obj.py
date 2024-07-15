from unittest.mock import Mock

import pytest

from storage.s3 import S3Storage


def test_success():
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.list_objects_v2.return_value = {
        "ResponseMetadata": {
            "RequestId": "21XR3DRD0Z8TCPS2",
            "HTTPStatusCode": 200,
            "RetryAttempts": 0,
        }
    }
    storage.save_object("test-folder/test-file-1.pkl", "test-content")


def test_invalid_path():
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()

    with pytest.raises(ValueError) as e:
        storage.save_object("test-folder/test-file-1.txt", "test-content")
    assert str(e.value) == "Path must end with '.pkl'"


# Need to work out how to test for invalid bucket properly...
