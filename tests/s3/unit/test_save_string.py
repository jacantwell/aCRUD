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
    response = storage.save_string("test-folder/test-file-1.txt", "test-content")


# Need to work out how to test for invalid bucket properly...
