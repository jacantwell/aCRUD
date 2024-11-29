import pytest
import os
import json
from unittest.mock import patch, mock_open
from typing import Any
import shutil


from acrud import create_storage, StorageConfig

local_config = StorageConfig({"storage_type": "local", "root": "."})
storage = create_storage(local_config)


class TestLocalStorage:
    @pytest.fixture
    def root_dir(self):
        """Create a LocalStorage instance with a temporary root directory."""
        return os.getcwd()

    @pytest.fixture()
    def setup_teardown(self, root_dir):
        """Setup and teardown for tests that need a temporary directory."""
        os.makedirs(root_dir, exist_ok=True)
        yield
        # NOTE: Cleanup commented out for debugging
        tmp_path = os.path.join(root_dir, "tmp")
        if os.path.exists(tmp_path):
            shutil.rmtree(tmp_path)

    def test_ping(self, root_dir):
        """Test the ping method returns correct response."""
        assert storage.ping() == {"response": "pong"}

    def test_create_file_simple(self, root_dir, setup_teardown):
        """Test creating a simple file without metadata."""
        test_data = "Hello, World!"
        file_path = "tmp/test.txt"

        storage.create_file(file_path, test_data)

        full_path = os.path.join(root_dir, file_path)
        assert os.path.exists(full_path)
        with open(full_path, "rb") as f:
            saved_data = f.read().decode()
        assert saved_data == test_data

    def test_create_file_with_metadata(self, root_dir, setup_teardown):
        """Test creating a file with metadata."""
        test_data = "Hello, World!"
        meta_data = {"author": "Test", "version": 1}
        file_path = "tmp/test.txt"

        storage.create_file(file_path, test_data, meta_data)

        # Check data file
        full_path = os.path.join(root_dir, file_path)
        assert os.path.exists(full_path)

        # Check metadata file - use the correct metadata path format
        filepath_without_ext = os.path.splitext(file_path)[0]
        meta_path = os.path.join(root_dir, f"{filepath_without_ext}_meta.json")
        print(meta_path)
        assert os.path.exists(meta_path)
        with open(meta_path, "rb") as f:
            saved_meta = json.loads(f.read().decode())
        assert saved_meta == meta_data

    def test_read_file_simple(self, root_dir, setup_teardown):
        """Test reading a simple file without metadata."""
        test_data = "Hello, World!"
        file_path = "tmp/test_no_meta.txt"  # Use a different filename to avoid metadata conflicts

        # Create test file
        full_path = os.path.join(root_dir, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(test_data.encode())

        data, meta_data = storage.read_file(file_path)
        assert data == test_data
        assert meta_data is None

    def test_read_file_with_metadata(self, root_dir, setup_teardown):
        """Test reading a file with metadata."""
        test_data = "Hello, World!"
        meta_data = {"author": "Test", "version": 1}
        file_path = "tmp/test_with_meta.txt"

        # Create test files
        full_path = os.path.join(root_dir, file_path)
        filepath_without_ext = os.path.splitext(file_path)[0]
        meta_path = os.path.join(root_dir, f"{filepath_without_ext}_meta.json")
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "wb") as f:
            f.write(test_data.encode())
        with open(meta_path, "wb") as f:
            f.write(json.dumps(meta_data).encode())

        data, read_meta_data = storage.read_file(file_path)
        assert data == test_data
        assert read_meta_data == meta_data

    def test_delete_file(self, root_dir, setup_teardown):
        """Test deleting a file and its metadata."""
        test_data = "Hello, World!"
        meta_data = {"author": "Test"}
        file_path = "tmp/test_delete.txt"

        # Create test file with metadata
        storage.create_file(file_path, test_data, meta_data)

        # Delete file
        storage.delete_file(file_path)

        # Verify both data and metadata files are deleted
        full_path = os.path.join(root_dir, file_path)
        filepath_without_ext = os.path.splitext(file_path)[0]
        meta_path = os.path.join(root_dir, f"{filepath_without_ext}_meta.json")
        assert not os.path.exists(full_path)
        assert not os.path.exists(meta_path)

    def test_list_files_in_directory(self, root_dir, setup_teardown):
        """Test listing files in a directory."""
        # Create a separate test directory
        test_dir = os.path.join(root_dir, "tmp/test_files")
        os.makedirs(test_dir, exist_ok=True)

        # Create test files in the test directory
        test_files = ["test1.txt", "test2.txt", "test3.json"]
        for file in test_files:
            file_path = os.path.join("tmp/test_files", file)
            storage.create_file(file_path, "content")

        files = storage.list_files_in_directory("tmp/test_files")
        expected_files = {os.path.splitext(f)[0] for f in test_files}
        assert set(files) == expected_files

    def test_list_subdirectories(self, root_dir, setup_teardown):
        """Test listing subdirectories."""
        # Create a separate test directory
        test_dir = os.path.join(root_dir, "tmp/test_dirs")
        os.makedirs(test_dir, exist_ok=True)

        # Create test directories
        test_dirs = ["dir1", "dir2", "dir3"]
        for dir_name in test_dirs:
            os.makedirs(os.path.join(test_dir, dir_name))

        # Create a hidden directory
        os.makedirs(os.path.join(test_dir, ".hidden"))

        dirs = storage.list_subdirectories_in_directory("tmp/test_dirs")
        assert set(dirs) == set(test_dirs)
