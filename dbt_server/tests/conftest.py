from pytest import fixture
from unittest.mock import Mock


@fixture
def MockCloudStorage():
    mock_blob_size = Mock(return_value=10)
    mock_blob = Mock(name="mock_blob")
    mock_blob.upload_from_string.return_value = None
    mock_blob.download_as_bytes.return_value = b'hello world'
    mock_blob.name = 'test'
    mock_blob.len.return_value = 10
    mock_blob.size = mock_blob_size()

    mock_bucket = Mock(name="mock_bucket")
    mock_bucket.blob.return_value = mock_blob
    mock_bucket.get_blob.return_value = mock_blob

    mock_gcs_client = Mock(name="mock_gcs_client")
    mock_gcs_client.get_bucket.return_value = mock_bucket
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_gcs_client.list_blobs.return_value = [mock_blob]

    return mock_gcs_client, mock_bucket, mock_blob, mock_blob_size


@fixture
def MockState():
    mock_get = Mock(name='mock_get')
    mock_get.to_dict.return_value = {"log_starting_byte": 0}

    mock_document = Mock(name="mock_document")
    mock_document.set.return_value = None
    mock_document.get.return_value = mock_get

    mock_dbt_collection = Mock(name="mock_dbt_collection")
    mock_dbt_collection.document.return_value = mock_document

    return mock_dbt_collection, mock_document


@fixture
def MockLogging():
    mock_logger = Mock(name="mock_logger")

    mock_logging = Mock(name="mock_logging")
    mock_logging.logger.return_value = mock_logger

    return mock_logging
