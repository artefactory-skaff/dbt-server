import pytest
from unittest.mock import Mock, patch
from api.lib.cloud_storage import CloudStorage, GoogleCloudStorage, AzureBlobStorage, CloudStorageFactory


class TestCloudStorage:
    @pytest.fixture
    def service(self):
        return Mock()

    @pytest.fixture
    def cloud_storage(self, service):
        return CloudStorage(service)

    def test_write_file(self, cloud_storage, service):
        cloud_storage.write_file('bucket', 'file', 'data')
        service.write_file.assert_called_with('bucket', 'file', 'data')

    def test_get_file(self, cloud_storage, service):
        cloud_storage.get_file('bucket', 'file')
        service.get_file.assert_called_with('bucket', 'file', 0)

    def test_get_file_console_url(self, cloud_storage, service):
        cloud_storage.get_file_console_url('bucket', 'file')
        service.get_file_console_url.assert_called_with('bucket', 'file')

    def test_get_files_in_folder(self, cloud_storage, service):
        cloud_storage.get_files_in_folder('bucket', 'folder')
        service.get_files_in_folder.assert_called_with('bucket', 'folder')


class TestGoogleCloudStorage:
    @patch('api.lib.cloud_storage.storage')
    def setup(self, mock_storage):
        self.gcs = GoogleCloudStorage()

    # Add your tests here, similar to the CloudStorage tests, but specific to GoogleCloudStorage


class TestAzureBlobStorage:
    @patch('api.lib.cloud_storage.BlobServiceClient')
    def setup(self, mock_blob_service_client):
        self.abs = AzureBlobStorage()

    # Add your tests here, similar to the CloudStorage tests, but specific to AzureBlobStorage


class TestCloudStorageFactory:

    @patch('api.lib.cloud_storage.storage')
    def test_create_google_cloud_storage(self, mock_gcp_storage):
        mock_gcp_storage.Client.return_value = None
        service = CloudStorageFactory.create('GoogleCloudStorage')
        assert isinstance(service, GoogleCloudStorage)

    @patch('api.lib.cloud_storage.BlobServiceClient')
    @patch('api.lib.cloud_storage.settings')
    def test_create_azure_blob_storage(self, settings, mock_azure_client):
        settings.azure.blob_storage_connection_string = None
        mock_azure_client.from_connection_string.return_value = None
        service = CloudStorageFactory.create('AzureBlobStorage')
        assert isinstance(service, AzureBlobStorage)

    def test_create_invalid_service(self):
        with pytest.raises(ValueError):
            CloudStorageFactory.create('InvalidService')
