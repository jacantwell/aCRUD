import os
import io
from typing import Optional, Tuple, Any
import tempfile

from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from ..base import StorageBase, StorageConfig
from ..convert import convert, get_type
from .. import utils


# Load environment variables
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TOKEN_URI = "https://oauth2.googleapis.com/token.json"


class GoogleDriveStorageConfig(StorageConfig):
    # TODO: Should all of these values be here?
    client_id: str = CLIENT_ID
    client_secret: str = CLIENT_SECRET
    token: str
    token_uri: str = TOKEN_URI
    root_folder_id: Optional[str] = None


class GoogleDriveStorage(StorageBase):
    def __init__(self, config: GoogleDriveStorageConfig) -> None:
        """
        Initialize Google Drive API client using OAuth credentials.

        Args:
            storage_config (dict): Configuration dictionary containing:
                {
                    "storage_type": "google_drive",
                    "storage_config": {
                        "token": "...",
                        "token_uri": "...",
                        "root_folder_id": "..." # Optional
                    }
                }
        """

        # Initialize root_folder_id
        self.root_folder_id = config.root_folder_id

        # Create credentials from the token
        self.creds = Credentials(
            client_id=config.client_id,
            client_secret=config.client_secret,
            token=config.token,
            token_uri=config.token_uri,
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )

        self.service = build("drive", "v3", credentials=self.creds)

    def ping(self) -> dict:
        return {"response": "pong"}

    def create_file(
        self, file_path: str, data: Any, meta_data: Optional[dict] = None
    ) -> None:
        """
        Save a file to Google Drive.
        The data must be of a supported type.
        The meta data, if provided, must be a dictionary.
        If the file_path is `example/data.csv`, the meta data will be saved to `example/meta.json`.

        Args:
            file_path (str): The path to the file.
            data (Any): The data to save.
            meta_data (Optional[dict], optional): The meta data to save.

        Returns:
            None
        """
        # Create a temporary file to store the data
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Convert data to bytes and write to temp file
            byte_data = convert(data, bytes)
            temp_file.write(byte_data)
            temp_file_path = temp_file.name

        try:
            # Prepare file metadata
            file_metadata = {"name": os.path.basename(file_path)}
            if self.root_folder_id:
                file_metadata["parents"] = [self.root_folder_id]

            # Upload the main file
            media = MediaFileUpload(temp_file_path, resumable=True)
            self.service.files().create(
                body=file_metadata, media_body=media, fields="id"
            ).execute()

            # Handle metadata if provided
            if meta_data is not None:
                meta_data_file_path = utils.get_meta_data_file_path(file_path)
                meta_file_metadata = {"name": os.path.basename(meta_data_file_path)}
                if self.root_folder_id:
                    meta_file_metadata["parents"] = [self.root_folder_id]

                # Create temporary file for metadata
                with tempfile.NamedTemporaryFile(delete=False) as meta_temp_file:
                    meta_bytes = convert(meta_data, bytes)
                    meta_temp_file.write(meta_bytes)
                    meta_temp_path = meta_temp_file.name

                try:
                    meta_media = MediaFileUpload(meta_temp_path, resumable=True)
                    self.service.files().create(
                        body=meta_file_metadata, media_body=meta_media, fields="id"
                    ).execute()
                finally:
                    os.unlink(meta_temp_path)

        finally:
            os.unlink(temp_file_path)

    def read_file(self, file_path: str) -> Tuple[Any, Optional[dict]]:
        """
        Read a file from Google Drive.
        The data will be converted to the appropriate type.
        The meta data, if provided, will be converted to a dictionary.

        Args:
            file_path (str): The path to the file.

        Returns:
            Tuple[Any, Optional[dict]]: The data and, if available, the meta data.
        """
        # Find the file by name
        file_name = os.path.basename(file_path)
        response = (
            self.service.files()
            .list(q=f"name = '{file_name}'", spaces="drive", fields="files(id, name)")
            .execute()
        )

        if not response["files"]:
            raise FileNotFoundError(f"File {file_path} not found in Google Drive")

        file_id = response["files"][0]["id"]

        # Read the main file
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while done is False:
            _, done = downloader.next_chunk()

        # Convert the data to the appropriate type
        data = convert(fh.getvalue(), get_type(file_path))

        # Try to get metadata
        meta_data = None
        try:
            meta_data_file_path = utils.get_meta_data_file_path(file_path)
            meta_file_name = os.path.basename(meta_data_file_path)

            response = (
                self.service.files()
                .list(
                    q=f"name = '{meta_file_name}'",
                    spaces="drive",
                    fields="files(id, name)",
                )
                .execute()
            )

            if response["files"]:
                meta_file_id = response["files"][0]["id"]
                request = self.service.files().get_media(fileId=meta_file_id)
                meta_fh = io.BytesIO()
                downloader = MediaIoBaseDownload(meta_fh, request)

                done = False
                while done is False:
                    _, done = downloader.next_chunk()

                meta_data = convert(meta_fh.getvalue(), dict)

        except Exception as e:
            print(f"No metadata found: {str(e)}")
            meta_data = None

        return data, meta_data

    def delete_file(self, file_path: str) -> None:
        """
        Delete a file and its metadata from Google Drive.
        If the file_path is `example/data.csv`, both `example/data.csv` and `example/meta.json` will be deleted if they exist.

        Args:
            file_path (str): The path to the file to delete.

        Returns:
            None
        """
        # Find and delete the main file
        file_name = os.path.basename(file_path)
        response = (
            self.service.files()
            .list(q=f"name = '{file_name}'", spaces="drive", fields="files(id, name)")
            .execute()
        )

        if not response["files"]:
            raise FileNotFoundError(f"File {file_path} not found in Google Drive")

        file_id = response["files"][0]["id"]
        self.service.files().delete(fileId=file_id).execute()

        # Try to find and delete the metadata file if it exists
        try:
            meta_data_file_path = utils.get_meta_data_file_path(file_path)
            meta_file_name = os.path.basename(meta_data_file_path)

            response = (
                self.service.files()
                .list(
                    q=f"name = '{meta_file_name}'",
                    spaces="drive",
                    fields="files(id, name)",
                )
                .execute()
            )

            if response["files"]:
                meta_file_id = response["files"][0]["id"]
                self.service.files().delete(fileId=meta_file_id).execute()

        except Exception as e:
            print(f"No metadata file found to delete: {str(e)}")

    def list_files_in_directory(self, file_path: str) -> list:
        pass

    def list_subdirectories_in_directory(self, file_path) -> list:
        pass
