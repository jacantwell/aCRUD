from io import BytesIO
from unittest.mock import Mock

import dill
import pytest
from botocore.response import StreamingBody

from storage.s3 import S3Storage


@pytest.fixture
def obj_response():
    obj = {"a": 1, "b": 2}
    pkl_obj = dill.dumps(obj)
    body = StreamingBody(BytesIO(pkl_obj), len(pkl_obj))
    mocked_response = {
        "Body": body,
    }
    return obj, mocked_response


def test_success(obj_response):
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    obj, obj_response_val = obj_response
    storage.client.get_object.return_value = obj_response_val
    data = storage.load_object("test-folder/test.pkl")
    assert data == obj


def test_invalid_bucket(no_such_bucket_response):
    config = {"bucket": "invalid-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.get_object.side_effect = no_such_bucket_response
    with pytest.raises(LookupError) as e:
        storage.load_object("test-folder/test.pkl")
    assert str(e.value) == "Bucket `invalid-bucket` not found"


def test_invalid_file_path():
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    with pytest.raises(ValueError) as e:
        storage.save_object("test-folder/test-file-1.txt", "test-content")
    assert str(e.value) == "Path must end with '.pkl'"


def test_invalid_path(no_such_key_response):
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.get_object.side_effect = no_such_key_response
    storage.client.list_objects_v2.return_value = {"Contents": []}
    with pytest.raises(LookupError) as e:
        storage.load_object("test-folder/test.pkl")
    assert (
        str(e.value)
        == "Unable to find directory `test-folder/test.pkl` in your account"
    )
