import pytest

from botocore.exceptions import ClientError


@pytest.fixture
def no_such_bucket_response():
    error_response = {
        "Error": {
            "Code": "NoSuchBucket",
            "Message": "The specified bucket does not exist",
        }
    }
    operation_name = "GetObject"
    return ClientError(error_response, operation_name)


@pytest.fixture
def no_such_key_response():
    error_response = {
        "Error": {
            "Code": "NoSuchKey",
            "Message": "The specified key does not exist",
        }
    }
    operation_name = "GetObject"
    return ClientError(error_response, operation_name)
