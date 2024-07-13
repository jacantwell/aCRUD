from io import BytesIO
from unittest.mock import Mock

import pytest
from botocore.response import StreamingBody

from storage.s3 import S3Storage


@pytest.fixture
def string_response():
    response = "a string"
    body_encoded = response.encode("utf-8")
    body = StreamingBody(BytesIO(body_encoded), len(body_encoded))
    mocked_response = {
        "Body": body,
    }
    return mocked_response


def test_success(string_response):
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.get_object.return_value = string_response
    data = storage.load_string("test-folder/test.csv")
    assert data == "a string"


# Again, need to figure out how to properly catch and test botocore exceptions
