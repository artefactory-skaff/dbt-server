from api.lib.cloud_storage import CloudStorage


def test_get_blob_from_bucket(MockCloudStorage):
    mock_gcs_client, mock_bucket, mock_blob, mock_blob_size = MockCloudStorage

    gcs = CloudStorage(mock_gcs_client)
    gcs.get_blob_from_bucket('bucket_name', 'blob_name', 0)

    mock_gcs_client.get_bucket.assert_called_once_with('bucket_name')
    mock_bucket.blob.assert_called_once_with('blob_name')
    mock_bucket.get_blob.assert_called_once_with('blob_name')
    mock_blob_size.assert_called_once()
    mock_blob.download_as_bytes.assert_called_once_with(client=None, start=0)


def test_write_to_bucket(MockCloudStorage):
    mock_gcs_client, mock_bucket, mock_blob, _ = MockCloudStorage

    gcs = CloudStorage(mock_gcs_client)
    gcs.write_to_bucket('bucket_name', 'blob_name', 'data')

    mock_gcs_client.bucket.assert_called_once_with('bucket_name')
    mock_bucket.blob.assert_called_once_with('blob_name')
    mock_blob.upload_from_string.assert_called_once_with('data', num_retries=5, retry=None)


def test_get_all_blobs_from_folder(MockCloudStorage):
    mock_gcs_client, _, mock_blob, _ = MockCloudStorage

    gcs = CloudStorage(mock_gcs_client)
    gcs.get_all_blobs_from_folder('bucket_name', 'folder_name')

    mock_gcs_client.list_blobs.assert_called_once_with('bucket_name', prefix='folder_name')
    mock_blob.download_as_bytes.assert_called_once_with(client=None)
