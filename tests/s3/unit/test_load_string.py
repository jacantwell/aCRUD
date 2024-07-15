from io import BytesIO
from unittest.mock import Mock

import pytest
from botocore.response import StreamingBody
from botocore.exceptions import ClientError

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


def test_invalid_bucket(no_such_bucket_response):
    config = {"bucket": "invalid-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.get_object.side_effect = no_such_bucket_response
    with pytest.raises(LookupError) as e:
        storage.load_string("test-folder/test.csv")
    assert str(e.value) == "Unable to save string. Bucket `invalid-bucket` not found"


def test_invalid_path(no_such_key_response):
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.get_object.side_effect = no_such_key_response
    storage.client.list_objects_v2.return_value = {"Contents": []}
    with pytest.raises(LookupError) as e:
        storage.load_string("test-folder/test.csv")
    assert (
        str(e.value)
        == "Unable to find directory `test-folder/test.csv` in your account"
    )
