from unittest.mock import Mock

import boto3.exceptions

import pytest
import boto3

from storage.s3 import S3Storage


def test_success():
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "test-bucket/test-folder/test-file-1.txt"},
        ]
    }

    files = storage.list_files_in_directory("test-folder")
    assert "test-file-1" in files


def test_no_files():
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.list_objects_v2.return_value = {"Contents": []}
    assert storage.list_files_in_directory("test-folder") == []


def test_no_bucket():
    config = {"bucket": "test-bucket"}
    storage = S3Storage(config)
    storage.client = Mock()
    storage.client.list_objects_v2.return_value = {"NoContents": []}

    with pytest.raises(LookupError) as e:
        storage.list_files_in_directory("test-user/test-folder")
    assert str(e.value) == f"Unable to find directory `test-user` in your account"
