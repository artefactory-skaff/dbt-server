from unittest.mock import Mock, patch

import pytest
from dbt_server.lib.storage import AzureBlobStorage, GoogleCloudStorage, Storage, StorageFactory


class TestGoogleCloudStorage:
    @pytest.fixture
    @patch("dbt_server.lib.storage.storage")
    def storage(self, mock_storage):
        return GoogleCloudStorage()

    @patch("dbt_server.lib.storage.GoogleCloudStorage")
    def test_write_file(self, google_cloud_storage, storage):
        google_cloud_storage.define_retry_policy.return_value = None
        storage.write_file("bucket", "file", "data")

    def test_get_file(self, storage):
        storage.client.get_bucket().get_blob().size = 1
        storage.get_file("bucket", "file")

    def test_get_file_console_url(self, storage):
        storage.get_file_console_url("bucket", "file")

    def test_get_files_in_folder(self, storage):
        storage.get_files_in_folder("bucket", "folder")


class TestAzureBlobStorage:
    @pytest.fixture
    @patch("dbt_server.lib.storage.BlobServiceClient")
    @patch("dbt_server.lib.storage.settings")
    def storage(self, settings, mock_blob_service_client):
        settings.azure.blob_storage_connection_string = None
        mock_blob_service_client.from_connection_string.return_value = None
        azure_blob_storage = AzureBlobStorage()
        azure_blob_storage.client = Mock()
        return azure_blob_storage

    def test_write_file(self, storage):
        storage.write_file("bucket", "file", "data")

    def test_get_file(self, storage):
        storage.client.get_blob_client().download_blob().readall.return_value = "test"
        storage.get_file("bucket", "file")

    def test_get_file_console_url(self, storage):
        storage.get_file_console_url("bucket", "file")

    def test_get_files_in_folder(self, storage):
        storage.client.get_container_client().list_blobs.return_value = []
        storage.get_files_in_folder("bucket", "folder")


class TestStorageFactory:
    @patch("dbt_server.lib.storage.storage")
    def test_create_google_cloud_storage(self, mock_gcp_storage):
        mock_gcp_storage.Client.return_value = None
        service = StorageFactory.create("GoogleCloudStorage")
        assert isinstance(service, GoogleCloudStorage)

    @patch("dbt_server.lib.storage.BlobServiceClient")
    @patch("dbt_server.lib.storage.settings")
    def test_create_azure_blob_storage(self, settings, mock_azure_client):
        settings.azure.blob_storage_connection_string = None
        mock_azure_client.from_connection_string.return_value = None
        service = StorageFactory.create("AzureBlobStorage")
        assert isinstance(service, AzureBlobStorage)

    def test_create_invalid_service(self):
        with pytest.raises(ValueError):
            StorageFactory.create("InvalidService")
