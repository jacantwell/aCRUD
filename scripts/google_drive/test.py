import os
from dotenv import load_dotenv

from acrud import create_storage, GoogleDriveStorageConfig


load_dotenv()


def create_storage_config(access_token):
    """Create a storage config using an access token."""

    google_credentials = {
        "token": access_token,
        "root_folder_id": None,  # Optional, set to specific folder ID if needed
    }
    config = GoogleDriveStorageConfig(**google_credentials)
    return config


def test_google_drive_storage():
    # Your access token
    access_token = os.getenv("GOOGLE_DRIVE_ACCESS_TOKEN")

    # Create storage config
    config = create_storage_config(access_token)

    storage = create_storage("google_drive", config)

    # Create a test file
    with open("test_file.txt", "w") as f:
        f.write("Hello Google Drive!")

    try:
        # Test file creation
        print("Testing file creation...")
        result = storage.create_file("test_file.txt", "Hello Google Drive!")
        print(f"File created")

        # Test file reading
        print("\nTesting file reading...")
        content = storage.read_file("test_file.txt")
        print(f"File content: {content}")

    finally:
        # Cleanup local test files
        if os.path.exists("test_file.txt"):
            os.remove("test_file.txt")
        if os.path.exists("test_file_updated.txt"):
            os.remove("test_file_updated.txt")


if __name__ == "__main__":
    test_google_drive_storage()
